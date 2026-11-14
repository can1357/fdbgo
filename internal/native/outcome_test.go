package native

import (
	"context"
	"sync"
	"testing"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/ryw"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestReadVersionWaitAndCancellation(t *testing.T) {
	s := &TxnShared{rvStarted: true, rvReady: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ReadVersion(ctx); err != fdberr.OperationCancelled {
		t.Fatalf("cancelled read version: %v", err)
	}

	want := ReadVersion{Version: 42}
	s.completeReadVersion(want, nil)
	got, err := s.ReadVersion(context.Background())
	if err != nil || got.Version != want.Version {
		t.Fatalf("completed read version = %+v, %v", got, err)
	}
	if err := s.SetReadVersion(43); err != fdberr.ReadVersionAlreadySet {
		t.Fatalf("setting a started read version: %v", err)
	}
}

func TestReaderAttemptCancellation(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	s := &TxnShared{ctx: ctx, cancel: cancel}
	r := Reader{shared: s}

	readCtx, done := r.readContext(context.Background())
	defer done()
	s.end()
	<-readCtx.Done()
	if err := cancelErr(readCtx); err != fdberr.TransactionCancelled {
		t.Fatalf("in-flight read after end: %v", err)
	}
	if _, err := r.Get(context.Background(), []byte("key")); err != fdberr.TransactionCancelled {
		t.Fatalf("read after end: %v", err)
	}
}

func TestStartCommitDoesNotRequestVersionForInvalidOrEmptyPlans(t *testing.T) {
	s := &TxnShared{}
	txn := &NativeTxn{Shared: s, Opts: DefaultNativeOptions()}
	txn.StartCommit(ryw.CommitPlan{})
	if s.rvStarted {
		t.Fatal("read-only commit without watches requested a read version")
	}
	txn.Opts.ReadOnly = true
	txn.StartCommit(ryw.CommitPlan{Mutations: []wire.Mutation{{}}})
	if s.rvStarted {
		t.Fatal("invalid commit requested a read version")
	}
}

func TestWatchCancelledBeforeCommit(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	db := &DbContext{ctx: context.Background(), Watches: newWatches()}
	s := &TxnShared{ctx: ctx, cancel: cancel, DB: db}
	r := Reader{shared: s, params: &readParams{}}
	ch, _, err := r.RegisterWatch([]byte("key"), nil)
	if err != nil {
		t.Fatal(err)
	}
	s.end()
	if err := <-ch; err != fdberr.TransactionCancelled {
		t.Fatalf("watch after end: %v", err)
	}
	if got := db.Watches.outstanding.Load(); got != 0 {
		t.Fatalf("outstanding watches = %d", got)
	}
}

func TestOutcomeWaitCancellationDoesNotDecideOutcome(t *testing.T) {
	s := &TxnShared{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Outcome(ctx); err != fdberr.OperationCancelled {
		t.Fatalf("cancelled outcome wait: %v", err)
	}
	want := CommitOutcome{State: OutcomeCommitted}
	s.setOutcome(want)
	if got, err := s.Outcome(context.Background()); err != nil || got != want {
		t.Fatalf("outcome after cancelled wait = %+v, %v", got, err)
	}
}

func TestCommitOutcomeObservers(t *testing.T) {
	for _, want := range []CommitOutcome{
		{State: OutcomeCommitted, Versionstamp: versionstamp(1234, 7), HasVersionstamp: true},
		{State: OutcomeCommitted},
		{State: OutcomeFailed},
	} {
		for range 100 {
			s := &TxnShared{}
			start := make(chan struct{})
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					<-start
					got, err := s.Outcome(context.Background())
					if err != nil || got != want {
						t.Errorf("outcome = %+v, %v; want %+v", got, err, want)
					}
				})
			}
			close(start)
			s.setOutcome(want)
			wg.Wait()
			// A late observer must see the same decision, including the
			// versionstamp. A second publisher cannot replace it.
			s.setOutcome(CommitOutcome{State: OutcomeFailed})
			if got, err := s.Outcome(context.Background()); err != nil || got != want {
				t.Fatalf("late outcome = %+v, %v; want %+v", got, err, want)
			}
			if got := s.OutcomeNow(); got != want {
				t.Fatalf("outcome now = %+v; want %+v", got, want)
			}
		}
	}
}
