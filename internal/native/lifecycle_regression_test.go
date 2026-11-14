package native

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/ryw"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestBreakVersionstampPreservesAttempt(t *testing.T) {
	db := newLifecycleTestDB(t, wire.Endpoint{})
	tr := NewNativeTxn(db, DefaultNativeOptions())
	defer tr.Close()
	r := ryw.New(ryw.NewRywOptions(1))
	tr.Shared.BindAttempt(r.ResetSignal())
	ch, _, err := tr.Reader().RegisterWatch([]byte("key"), nil)
	if err != nil {
		t.Fatal(err)
	}
	tr.Shared.BreakVersionstamp()
	if out, err := tr.Shared.Outcome(context.Background()); out.State != OutcomeFailed || err != fdberr.BrokenPromise {
		t.Fatalf("broken versionstamp = %+v, %v", out, err)
	}
	if err := tr.Shared.Context().Err(); err != nil {
		t.Fatalf("breaking versionstamp ended native reads: %v", err)
	}
	select {
	case err := <-ch:
		t.Fatalf("breaking versionstamp ended watch: %v", err)
	default:
	}
	r.Cancel()
	tr.Close()
	if err := <-ch; err != fdberr.TransactionCancelled {
		t.Fatalf("watch after reset cancellation = %v", err)
	}
	if _, err := tr.Shared.Outcome(context.Background()); err != fdberr.BrokenPromise {
		t.Fatalf("reset cancellation replaced broken_promise: %v", err)
	}
}

func TestPublishCommitReadOnlyWatchErrorDoesNotFailCommit(t *testing.T) {
	db := newLifecycleTestDB(t, wire.Endpoint{})
	tr := NewNativeTxn(db, DefaultNativeOptions())
	defer tr.Close()
	s := tr.Shared
	s.rvStarted, s.rvReady = true, make(chan struct{})
	s.completeReadVersion(ReadVersion{}, fdberr.TransactionTooOld)
	ch, _, err := tr.Reader().RegisterWatch([]byte("key"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishCommit(s.ctx, s, invalidVersion, [10]byte{}, false, nil); err != nil {
		t.Fatalf("read-only commit inherited watch GRV error: %v", err)
	}
	if out, err := s.Outcome(context.Background()); out.State != OutcomeCommitted || out.HasVersionstamp || err != nil {
		t.Fatalf("read-only outcome = %+v, %v", out, err)
	}
	if err := <-ch; err != fdberr.TransactionTooOld {
		t.Fatalf("watch GRV error = %v", err)
	}
	if got := db.Watches.outstanding.Load(); got != 0 {
		t.Fatalf("failed watch retains %d reservations", got)
	}
}

func TestPublishCommitReturnsSelectedAttemptError(t *testing.T) {
	for _, ended := range []fdberr.Error{fdberr.TransactionCancelled, fdberr.TransactionTimedOut} {
		for _, bound := range []bool{false, true} {
			for _, result := range []string{"write", "read-only", "commit-error"} {
				t.Run(fmt.Sprintf("%v/bound=%v/%s", ended, bound, result), func(t *testing.T) {
					db := newLifecycleTestDB(t, wire.Endpoint{})
					tr := NewNativeTxn(db, DefaultNativeOptions())
					defer tr.Close()
					s := tr.Shared
					if err := s.SetReadVersion(41); err != nil {
						t.Fatal(err)
					}
					ch, _, err := tr.Reader().RegisterWatch([]byte("key"), nil)
					if err != nil {
						t.Fatal(err)
					}
					if bound {
						r := ryw.New(ryw.NewRywOptions(1))
						s.BindAttempt(r.ResetSignal())
						r.Abort(ended)
					} else {
						s.endWithError(ended)
					}
					var commitErr error
					if result == "commit-error" {
						commitErr = fdberr.NotCommitted
					}
					err = publishCommit(s.ctx, s, 42, versionstamp(42, 0), result == "write", commitErr)
					if err != ended {
						t.Fatalf("commit = %v; want %v", err, ended)
					}
					if out, err := s.Outcome(context.Background()); out.State != OutcomeFailed || err != ended {
						t.Fatalf("outcome = %+v, %v", out, err)
					}
					if err := <-ch; err != ended {
						t.Fatalf("watch = %v; want %v", err, ended)
					}
					wantVersion := int64(invalidVersion)
					if result == "write" {
						wantVersion = 42
					}
					if got := s.CommittedVersion.Load(); got != wantVersion {
						t.Fatalf("actual committed version = %d; want %d", got, wantVersion)
					}
					if got := db.Watches.outstanding.Load(); got != 0 {
						t.Fatalf("failed watch retains %d reservations", got)
					}
				})
			}
		}
	}
}

func TestPublishCommitTransfersWatchesBeforeAttemptEnd(t *testing.T) {
	for _, wrote := range []bool{false, true} {
		for _, ended := range []fdberr.Error{fdberr.TransactionCancelled, fdberr.TransactionTimedOut} {
			t.Run(fmt.Sprintf("wrote=%v/%v", wrote, ended), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					db := newLifecycleTestDB(t, wire.Endpoint{})
					tr := NewNativeTxn(db, DefaultNativeOptions())
					defer tr.Close()
					s := tr.Shared
					r := ryw.New(ryw.NewRywOptions(1))
					s.BindAttempt(r.ResetSignal())
					if err := s.SetReadVersion(41); err != nil {
						t.Fatal(err)
					}
					started, resume := make(chan struct{}), make(chan struct{})
					wantVersion := int64(41)
					if wrote {
						wantVersion = 42
					}
					// Pause the first arm after successful publication, leaving the
					// real watch detached but not yet handed to the database.
					s.watches = append(s.watches, pendingWatch{arm: func(version int64, err error) {
						if version != wantVersion || err != nil {
							t.Errorf("first arm = %d, %v", version, err)
						}
						close(started)
						<-resume
					}})
					ch, finish, err := tr.Reader().RegisterWatch([]byte("key"), nil)
					if err != nil {
						t.Fatal(err)
					}
					committed := make(chan error, 1)
					go func() { committed <- publishCommit(s.ctx, s, 42, versionstamp(42, 0), wrote, nil) }()
					<-started
					if out, err := s.Outcome(context.Background()); out.State != OutcomeCommitted || err != nil {
						t.Fatalf("published outcome = %+v, %v", out, err)
					}
					r.Abort(ended)
					s.endWithError(ended)
					close(resume)
					if err := <-committed; err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
					select {
					case err := <-ch:
						t.Fatalf("committed watch ended with attempt: %v", err)
					default:
					}
					db.Watches.mu.Lock()
					m := db.Watches.m["key"]
					owned := m != nil && m.version == wantVersion && m.waiters == 1
					db.Watches.mu.Unlock()
					if !owned || db.Watches.outstanding.Load() != 1 {
						t.Fatal("committed watch was not transferred to the database")
					}
					finish(nil)
					if err := <-ch; err != nil {
						t.Fatal(err)
					}
					db.wg.Wait()
				})
			})
		}
	}
}

func TestReadOnlyCommitWaitsForWatchVersionBeforePublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := newLifecycleTestDB(t, wire.Endpoint{})
		tr := NewNativeTxn(db, DefaultNativeOptions())
		defer tr.Close()
		s := tr.Shared
		r := ryw.New(ryw.NewRywOptions(1))
		s.BindAttempt(r.ResetSignal())
		s.rvStarted, s.rvReady = true, make(chan struct{})
		ch, _, err := tr.Reader().RegisterWatch([]byte("key"), nil)
		if err != nil {
			t.Fatal(err)
		}
		committed := make(chan error, 1)
		go func() { committed <- publishCommit(s.ctx, s, invalidVersion, [10]byte{}, false, nil) }()
		synctest.Wait()
		if out := s.OutcomeNow(); out.State != OutcomePending {
			t.Fatalf("published before watch version was ready: %+v", out)
		}
		r.Abort(fdberr.TransactionTimedOut)
		if err := <-committed; err != fdberr.TransactionTimedOut {
			t.Fatalf("commit after timeout = %v", err)
		}
		if err := <-ch; err != fdberr.TransactionTimedOut {
			t.Fatalf("watch after timeout = %v", err)
		}
	})
}

func TestGRVUpdaterCannotStartAfterClose(t *testing.T) {
	db := newLifecycleTestDB(t, wire.Endpoint{})
	// Hold start before registration while shutdown runs to completion.
	db.grvCacheMu.Lock()
	started, done := make(chan struct{}), make(chan struct{})
	go func() {
		close(started)
		startGRVUpdater(db)
		close(done)
	}()
	<-started
	db.Close()
	db.grvCacheMu.Unlock()
	<-done
	if db.grvCacheSnapshot().UpdaterRunning {
		t.Fatal("updater registered after Close returned")
	}
}

func TestDbCloseJoinsRegisteredBackgroundTasks(t *testing.T) {
	db := newLifecycleTestDB(t, wire.Endpoint{})
	if !db.addBackground() {
		t.Fatal("open database rejected registration")
	}
	closed := make(chan struct{})
	go func() {
		db.Close()
		close(closed)
	}()
	<-db.Context().Done()
	if db.addBackground() {
		db.wg.Done()
		t.Error("closing database accepted registration")
	}
	select {
	case <-closed:
		t.Error("Close returned before registered work finished")
	default:
	}
	db.wg.Done()
	<-closed
}

func TestCachedLocationsReplacementKeepsSnapshot(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%v", reverse), func(t *testing.T) {
			c := newLocationCache()
			c.Insert(loc("a", "m"))
			c.Insert(loc("m", "z"))
			before, ok := cachedLocations(c, []byte("b"), []byte("y"), 10, reverse)
			if !ok || len(before) != 2 {
				t.Fatalf("initial snapshot = %+v, %v", before, ok)
			}
			// Replacing shards must neither mutate an existing snapshot nor
			// leave overlapping locations in a later traversal.
			inserted := make(chan struct{})
			go func() {
				c.Insert(loc("c", "x"))
				close(inserted)
			}()
			<-inserted
			after, ok := cachedLocations(c, []byte("b"), []byte("y"), 10, reverse)
			if !ok || len(after) != 3 {
				t.Fatalf("replacement snapshot = %+v, %v", after, ok)
			}
			for _, snapshot := range [][]Location{before, after} {
				for i, l := range snapshot {
					if bytes.Compare(l.Begin, l.End) >= 0 {
						t.Fatalf("empty shard: %+v", l)
					}
					if i > 0 {
						left, right := snapshot[i-1], l
						if reverse {
							left, right = right, left
						}
						if !bytes.Equal(left.End, right.Begin) {
							t.Fatalf("inconsistent cached snapshot: %+v", snapshot)
						}
					}
				}
			}
		})
	}
}
