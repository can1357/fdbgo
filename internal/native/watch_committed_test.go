package native

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestCausalReadVerificationAfterRecovery(t *testing.T) {
	for _, tc := range []struct {
		name        string
		percent     int64
		provisional bool
		emptyOld    bool
		emptyNew    bool
		unchanged   bool
		wantProbe   bool
	}{
		{name: "recovered", percent: 100, wantProbe: true},
		{name: "disabled"},
		{name: "provisional", percent: 100, provisional: true},
		{name: "initial", percent: 100, emptyOld: true},
		{name: "unavailable", percent: 100, emptyNew: true},
		{name: "unchanged", percent: 100, unchanged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := make(chan wire.UID, 1)
			server := newNativeTestServer(t, func(c *nativeTestConn, dest wire.UID, msg []byte) {
				request, err := lifecycleRequestTable(msg)
				if err != nil {
					t.Error(err)
					return
				}
				count, countErr := request.U32(0)
				flags, flagsErr := request.U32(1)
				if countErr != nil || flagsErr != nil || count != 1 ||
					flags != PriorityImmediate.GRVFlag()|grvCausalReadRisky {
					t.Errorf("verification GRV: count %d flags %#x errors %v, %v", count, flags, countErr, flagsErr)
				}
				requests <- dest
				token, err := nativeRequestReplyToken(msg, 5)
				if err == nil {
					err = c.send(token, lifecycleGRVReply(42, nil))
				}
				if err != nil {
					t.Error(err)
				}
			})
			old := wire.Endpoint{Addr: server.addr(), Token: wire.UID{First: 123, Second: 100}}
			initial := old
			if tc.emptyOld {
				initial = wire.Endpoint{}
			}
			db := newLifecycleTestDB(t, initial)
			db.SetCausalReadVerification(tc.percent)
			next := old.Adjusted(1)
			if tc.unchanged {
				next = old
			}
			info := &ClusterInfo{GRV: []wire.Endpoint{next}, Commit: []wire.Endpoint{old}, Provisional: tc.provisional}
			if tc.emptyNew {
				info.GRV = nil
			}
			db.publish(info)
			if tc.wantProbe {
				select {
				case dest := <-requests:
					if dest != old.Token {
						t.Fatalf("probed %v, want old proxy %v", dest, old.Token)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("old proxy was not verified after recovery")
				}
			}
			db.Close()
			select {
			case dest := <-requests:
				t.Fatalf("unexpected verification request to %v", dest)
			default:
			}
		})
	}
}

func checkCommittedGRVRequest(t *testing.T, msg []byte) {
	t.Helper()
	request, err := lifecycleRequestTable(msg)
	if err != nil {
		t.Error(err)
		return
	}
	count, countErr := request.U32(0)
	flags, flagsErr := request.U32(1)
	if countErr != nil || flagsErr != nil || count != 0 || flags != PriorityImmediate.GRVFlag() {
		t.Errorf("committed-version GRV count %d flags %#x errors %v, %v", count, flags, countErr, flagsErr)
	}
}

func TestWatchWaitsForCommittedVersion(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "behind"
		if recovery {
			name = "recovery-gap"
		}
		t.Run(name, func(t *testing.T) {
			const replyVersion = int64(150)
			const recoveredVersion = replyVersion + 50_000_000
			var watchCalls, grvCalls atomic.Int64
			var ep wire.Endpoint
			var previousGRV time.Time
			server := newNativeTestServer(t, func(c *nativeTestConn, dest wire.UID, msg []byte) {
				if dest == ep.Adjusted(ssWatchValue).Token {
					n := watchCalls.Add(1)
					request, err := lifecycleRequestTable(msg)
					if err != nil {
						t.Error(err)
						return
					}
					version, err := request.I64(3)
					want := int64(100)
					if n > 1 {
						want = recoveredVersion
					}
					if err != nil || version != want {
						t.Errorf("storage watch %d at version %d, %v; want %d", n, version, err, want)
					}
					token, err := nativeRequestReplyToken(msg, 8)
					if err != nil {
						t.Error(err)
						return
					}
					v := replyVersion
					if n > 1 {
						v = recoveredVersion + 1
					}
					if err := c.send(token, func(b *wire.Builder) []byte {
						b.StartTable(2)
						b.AddI64(0, v)
						return lifecycleReply(b, b.EndTable(), (&wire.WatchValue{}).ReplyFileID())
					}); err != nil {
						t.Error(err)
					}
					return
				}
				if dest != ep.Token {
					t.Errorf("unexpected endpoint: %v", dest)
					return
				}
				checkCommittedGRVRequest(t, msg)
				n := grvCalls.Add(1)
				if n == 2 && time.Since(previousGRV) < secs(futureVersionRetryDelay) {
					t.Error("GRV behind the watch retried without the future-version delay")
				}
				previousGRV = time.Now()
				token, err := nativeRequestReplyToken(msg, 5)
				if err != nil {
					t.Error(err)
					return
				}
				v := replyVersion
				if n == 1 {
					v = replyVersion - 1
				} else if recovery && n == 2 {
					v = recoveredVersion
				} else if recovery {
					v = recoveredVersion + 1
				}
				if err := c.send(token, lifecycleGRVReply(v, nil)); err != nil {
					t.Error(err)
				}
			})
			ep = wire.Endpoint{Addr: server.addr(), Token: wire.UID{First: 444, Second: 100}}
			db := newLifecycleTestDB(t, ep)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := watchValue(ctx, db, []byte("key"), []byte("A"), 100, nil, wire.UID{})
			want, wantWatches, wantGRVs := replyVersion, int64(1), int64(2)
			if recovery {
				want, wantWatches, wantGRVs = recoveredVersion+1, 2, 3
			}
			if err != nil || got != want || watchCalls.Load() != wantWatches || grvCalls.Load() != wantGRVs {
				t.Fatalf("watch = %d, %v; storage calls %d GRVs %d; want %d, %d, %d", got, err, watchCalls.Load(), grvCalls.Load(), want, wantWatches, wantGRVs)
			}
		})
	}
}

func TestWaitForCommittedVersionRetriesTransientGRVErrors(t *testing.T) {
	for _, code := range []fdberr.Error{fdberr.BatchTransactionThrottled, fdberr.GRVProxyMemoryLimitExceeded} {
		t.Run(code.Error(), func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			var first time.Time
			server := newNativeTestServer(t, func(c *nativeTestConn, _ wire.UID, msg []byte) {
				checkCommittedGRVRequest(t, msg)
				token, err := nativeRequestReplyToken(msg, 5)
				if err != nil {
					t.Error(err)
					return
				}
				reply := lifecycleGRVReply(100, nil)
				if calls.Add(1) == 1 {
					first = time.Now()
					reply = lifecycleErrorReply(code, (wire.GetReadVersion{}).ReplyFileID())
				} else if time.Since(first) < 4500*time.Millisecond {
					t.Error("GRV overload retried without the jittered GRV error delay")
				}
				if err := c.send(token, reply); err != nil {
					t.Error(err)
				}
			})
			ep := wire.Endpoint{Addr: server.addr(), Token: wire.UID{First: 555, Second: 100}}
			db := newLifecycleTestDB(t, ep)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			got, err := waitForCommittedVersion(ctx, db, 100)
			if err != nil || got != 100 || calls.Load() != 2 {
				t.Fatalf("committed version = %d, %v; requests %d", got, err, calls.Load())
			}
		})
	}
}

func TestWaitForCommittedVersionRestartsOnProxyChange(t *testing.T) {
	requested := make(chan struct{})
	var calls atomic.Int64
	server := newNativeTestServer(t, func(c *nativeTestConn, _ wire.UID, msg []byte) {
		checkCommittedGRVRequest(t, msg)
		if calls.Add(1) == 1 {
			close(requested)
			return
		}
		token, err := nativeRequestReplyToken(msg, 5)
		if err != nil {
			t.Error(err)
			return
		}
		if err := c.send(token, lifecycleGRVReply(100, nil)); err != nil {
			t.Error(err)
		}
	})
	ep := wire.Endpoint{Addr: server.addr(), Token: wire.UID{First: 666, Second: 100}}
	db := newLifecycleTestDB(t, ep)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		v, err := waitForCommittedVersion(ctx, db, 100)
		if err == nil && v != 100 {
			t.Errorf("committed version = %d", v)
		}
		result <- err
	}()
	select {
	case <-requested:
	case <-ctx.Done():
		t.Fatal("first GRV was not sent")
	}
	ep.Token.First++
	db.publish(&ClusterInfo{GRV: []wire.Endpoint{ep}, Commit: []wire.Endpoint{ep}})
	if err := <-result; err != nil || calls.Load() != 2 {
		t.Fatalf("proxy-change retry: %v, requests %d", err, calls.Load())
	}
}

func TestWaitForCommittedVersionPropagatesErrorsAndCancellation(t *testing.T) {
	for _, cancelWait := range []bool{false, true} {
		name := "fatal-error"
		if cancelWait {
			name = "cancelled-retry"
		}
		t.Run(name, func(t *testing.T) {
			replied := make(chan struct{})
			server := newNativeTestServer(t, func(c *nativeTestConn, _ wire.UID, msg []byte) {
				token, err := nativeRequestReplyToken(msg, 5)
				if err != nil {
					t.Error(err)
					return
				}
				code := fdberr.PermissionDenied
				if cancelWait {
					code = fdberr.GRVProxyMemoryLimitExceeded
				}
				if err := c.send(token, lifecycleErrorReply(code, (wire.GetReadVersion{}).ReplyFileID())); err != nil {
					t.Error(err)
				}
				close(replied)
			})
			ep := wire.Endpoint{Addr: server.addr(), Token: wire.UID{First: 777, Second: 100}}
			db := newLifecycleTestDB(t, ep)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := waitForCommittedVersion(ctx, db, 100)
				result <- err
			}()
			select {
			case <-replied:
			case <-ctx.Done():
				t.Fatal("GRV request was not received")
			}
			want := error(fdberr.PermissionDenied)
			if cancelWait {
				cancel()
				want = fdberr.OperationCancelled
			}
			if err := <-result; err != want {
				t.Fatalf("committed version error = %v; want %v", err, want)
			}
		})
	}
}
