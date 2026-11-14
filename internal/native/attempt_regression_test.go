package native

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/ryw"
)

func TestReadVersionPublicationRespectsAttemptEnd(t *testing.T) {
	for _, ended := range []fdberr.Error{fdberr.TransactionCancelled, fdberr.TransactionTimedOut} {
		for _, completeFirst := range []bool{false, true} {
			r := ryw.New(ryw.NewRywOptions(1))
			s := newTxnShared(context.Background(), nil, &NativeOptions{})
			s.BindAttempt(r.ResetSignal())
			s.rvStarted, s.rvReady = true, make(chan struct{})
			if completeFirst {
				s.completeReadVersion(ReadVersion{Version: 42}, nil)
			}
			r.Abort(ended)
			if !completeFirst {
				s.completeReadVersion(ReadVersion{Version: 42}, nil)
			}
			// The waiter starts only after both events: scheduling must not
			// turn a late GRV into success or invalidate an earlier success.
			rv, err := s.ReadVersion(context.Background())
			if completeFirst {
				if rv.Version != 42 || err != nil {
					t.Fatalf("completed GRV after %v = %+v, %v", ended, rv, err)
				}
			} else if err != ended {
				t.Fatalf("late GRV after %v = %+v, %v", ended, rv, err)
			}
			s.end()
		}
	}
}

func TestCommitOutcomePublicationRespectsAttemptEnd(t *testing.T) {
	for _, completeFirst := range []bool{false, true} {
		r := ryw.New(ryw.NewRywOptions(1))
		s := newTxnShared(context.Background(), nil, &NativeOptions{})
		s.BindAttempt(r.ResetSignal())
		out := CommitOutcome{State: OutcomeCommitted, HasVersionstamp: true, Versionstamp: versionstamp(42, 0)}
		if completeFirst {
			s.setOutcome(out)
		}
		r.Cancel()
		if !completeFirst {
			s.setOutcome(out)
		}
		got, err := s.Outcome(context.Background())
		if completeFirst {
			if got != out || err != nil {
				t.Fatalf("completed outcome after cancellation = %+v, %v", got, err)
			}
		} else if err != fdberr.TransactionCancelled {
			t.Fatalf("late outcome = %+v, %v", got, err)
		}
		s.end()
	}
}

type blockedEndSelector struct {
	*fakeShards
	started chan struct{}
	stopped chan struct{}
}

func (s *blockedEndSelector) resolveKey(ctx context.Context, sel kv.KeySelector) ([]byte, error) {
	if string(sel.Key) == "begin" {
		<-s.started
		return nil, fdberr.TransactionTooOld
	}
	close(s.started)
	<-ctx.Done()
	close(s.stopped)
	return nil, cancelErr(ctx)
}

func TestRangeFallbackBeginErrorCancelsEndSelector(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &blockedEndSelector{
			fakeShards: newFakeShards(t, []string{"", "\xff\xff"}),
			started:    make(chan struct{}), stopped: make(chan struct{}),
		}
		_, err := rangeFallback(context.Background(), s, s.family(),
			kv.FirstGreaterOrEqual([]byte("begin")), kv.FirstGreaterOrEqual([]byte("end")), kv.DefaultLimits(), false)
		if err != fdberr.TransactionTooOld {
			t.Fatalf("fallback = %v; want begin selector's error", err)
		}
		synctest.Wait()
		select {
		case <-s.stopped:
		default:
			t.Fatal("end selector remained blocked after begin failed")
		}
	})
}
