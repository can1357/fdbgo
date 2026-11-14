package ryw

import (
	"context"
	"encoding/binary"
	"testing"
	"testing/synctest"

	"github.com/can1357/fdbgo/internal/fdberr"
)

// failingTrackingReader returns a read error only after the fixture gate opens.
type failingTrackingReader struct{ *fakeDB }

func (db *failingTrackingReader) Get(ctx context.Context, key []byte) ([]byte, error) {
	if _, err := db.fakeDB.Get(ctx, key); err != nil {
		return nil, err
	}
	return nil, fdberr.AccessedUnreadable
}

func TestRecordedReadErrorDoesNotWaitForUnresolvedReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db, _ := newFakeDB(nil, false).gated(t)
		r := newRyw()
		defer r.Cancel()
		value := binary.LittleEndian.AppendUint32([]byte("0123456789ab"), 1)
		r.AtomicOp([]byte("k"), value, MutationSetVersionstampedValue, 0, false)
		_, err := await_(t, capture(r.Get(db, []byte("k"), false)))
		eqErr(t, err, fdberr.AccessedUnreadable)
		unresolved := capture(r.Get(db, []byte("q"), false))
		if unresolved.IsReady() {
			t.Fatal("unresolved read completed before its reply")
		}
		if wait, err := r.PendingReads(); wait != nil || err != fdberr.AccessedUnreadable {
			t.Fatalf("pending reads: wait %v error %v", wait != nil, err)
		}
		_, wait, err := r.Commit()
		if wait != nil || err != fdberr.AccessedUnreadable {
			t.Fatalf("commit: wait %v error %v", wait != nil, err)
		}
	})
}

func TestReadErrorWakesBlockedCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := newFakeDB(nil, false)
		held, _ := db.gated(t)
		failing, fail := db.gated(t)
		r := newRyw()
		defer r.Cancel()
		unresolved := capture(r.Get(held, []byte("q"), false))
		failed := capture(r.Get(&failingTrackingReader{failing}, []byte("k"), false))
		wait, err := r.PendingReads()
		if wait == nil || err != nil {
			t.Fatalf("pending reads: wait %v error %v", wait != nil, err)
		}
		pending := capture(struct{}{}, func(ctx context.Context) (struct{}, error) {
			return struct{}{}, wait(context.Background())
		}, nil)
		commit := capture(r.Commit())
		synctest.Wait()
		if commit.IsReady() || pending.IsReady() {
			t.Fatal("wait completed before either read")
		}
		fail()
		synctest.Wait()
		if !failed.IsReady() || !commit.IsReady() || !pending.IsReady() {
			t.Fatal("read failure did not wake commit and pending-read waiter")
		}
		_, err = await_(t, failed)
		eqErr(t, err, fdberr.AccessedUnreadable)
		_, err = await_(t, commit)
		eqErr(t, err, fdberr.AccessedUnreadable)
		_, err = await_(t, pending)
		eqErr(t, err, fdberr.AccessedUnreadable)
		if unresolved.IsReady() {
			t.Fatal("unrelated read must remain unresolved")
		}
	})
}

func TestPendingReadsWaiterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db, release := newFakeDB(nil, false).gated(t)
		r := newRyw()
		defer r.Cancel()
		read := capture(r.Get(db, []byte("q"), false))
		wait, err := r.PendingReads()
		if wait == nil || err != nil {
			t.Fatalf("pending reads: wait %v error %v", wait != nil, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pending := capture(struct{}{}, func(context.Context) (struct{}, error) {
			return struct{}{}, wait(ctx)
		}, nil)
		synctest.Wait()
		if pending.IsReady() {
			t.Fatal("wait completed before cancellation")
		}
		cancel()
		synctest.Wait()
		if !pending.IsReady() {
			t.Fatal("cancelled waiter remained blocked")
		}
		_, err = await_(t, pending)
		eqErr(t, err, context.Canceled)
		eqErr(t, r.ResetError(), nil)
		if read.IsReady() {
			t.Fatal("waiter cancellation ended its read")
		}
		release()
		must(t, read)
		eqErr(t, wait(context.Background()), nil)
		if wait, err := r.PendingReads(); wait != nil || err != nil {
			t.Fatalf("completed reads: wait %v error %v", wait != nil, err)
		}
		must(t, capture(r.Commit()))
	})
}

func TestPendingReadsCapturesIssuedSetAndErrors(t *testing.T) {
	for _, laterFails := range []bool{false, true} {
		name := "later-pending"
		if laterFails {
			name = "later-failure"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := newRyw()
				defer r.Cancel()
				earlier, finishEarlier := newFakeDB(nil, false).gated(t)
				first := capture(r.Get(earlier, []byte("a"), false))
				wait, err := r.PendingReads()
				if err != nil || wait == nil {
					t.Fatalf("captured reads: pending=%v, %v", wait != nil, err)
				}
				later, finishLater := newFakeDB(nil, false).gated(t)
				var reader SnapshotReader = later
				if laterFails {
					reader = &failingTrackingReader{later}
				}
				second := capture(r.Get(reader, []byte("z"), false))
				if laterFails {
					finishLater()
					_, err := await_(t, second)
					eqErr(t, err, fdberr.AccessedUnreadable)
				}
				pending := capture(struct{}{}, func(ctx context.Context) (struct{}, error) {
					return struct{}{}, wait(ctx)
				}, nil)
				synctest.Wait()
				if pending.IsReady() {
					t.Fatal("later read completed the captured wait before its read finished")
				}
				finishEarlier()
				must(t, first)
				synctest.Wait()
				if !pending.IsReady() {
					t.Fatal("later read extended the captured wait")
				}
				must(t, pending)
				if !laterFails {
					if second.IsReady() {
						t.Fatal("captured wait completed the later read")
					}
					finishLater()
					must(t, second)
				}
				eqErr(t, wait(context.Background()), nil)
			})
		})
	}
}

func TestPendingReadsWaiterStaysWithResetAttempt(t *testing.T) {
	for _, startBeforeReset := range []bool{false, true} {
		name := "reset-before-wait"
		if startBeforeReset {
			name = "reset-during-wait"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				db, release := newFakeDB(nil, false).gated(t)
				r := newRyw()
				defer r.Cancel()
				oldRead := capture(r.Get(db, []byte("old"), false))
				wait, err := r.PendingReads()
				if wait == nil || err != nil {
					t.Fatalf("pending reads: wait %v error %v", wait != nil, err)
				}
				var pending *testResult[struct{}]
				if startBeforeReset {
					pending = capture(struct{}{}, func(ctx context.Context) (struct{}, error) {
						return struct{}{}, wait(context.Background())
					}, nil)
					synctest.Wait()
					if pending.IsReady() {
						t.Fatal("wait completed before reset")
					}
				}
				r.Reset()
				newRead := capture(r.Get(db, []byte("new"), false))
				synctest.Wait()
				if pending != nil {
					if !pending.IsReady() {
						t.Fatal("reset did not wake old waiter")
					}
					_, err = await_(t, pending)
					eqErr(t, err, fdberr.TransactionCancelled)
				}
				eqErr(t, wait(context.Background()), fdberr.TransactionCancelled)
				_, err = await_(t, oldRead)
				eqErr(t, err, fdberr.TransactionCancelled)
				if newRead.IsReady() {
					t.Fatal("reset waiter affected new read")
				}
				release()
				must(t, newRead)
				must(t, capture(r.Commit()))
			})
		})
	}
}

func TestAttemptEndAbortDoesNotPoisonNewAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db, _ := newFakeDB(nil, false).gated(t)
		r := newRyw()
		old := r.ResetSignal()
		r.Reset()
		current := r.ResetSignal()
		read := capture(r.Get(db, []byte("q"), false))
		old.Abort(fdberr.TransactionTimedOut)
		eqErr(t, old.Err(), fdberr.TransactionCancelled)
		eqErr(t, current.Err(), nil)
		synctest.Wait()
		if read.IsReady() {
			t.Fatal("old abort ended the new attempt's read")
		}
		current.Abort(fdberr.TransactionTimedOut)
		current.Abort(fdberr.TransactionCancelled)
		select {
		case <-current.Done():
		default:
			t.Fatal("abort did not end the attempt")
		}
		_, err := await_(t, read)
		eqErr(t, err, fdberr.TransactionTimedOut)
		if wait, err := r.PendingReads(); wait != nil || err != fdberr.TransactionTimedOut {
			t.Fatalf("aborted reads: wait %v error %v", wait != nil, err)
		}
		r.Reset()
		current.Abort(fdberr.TransactionTimedOut)
		must(t, capture(r.Commit()))
	})
}

// observedPointReader distinguishes issue-time pinning from execution of the
// blocking body, and reports when the actual body has unwound.
type observedPointReader struct {
	*recordingReader
	entered chan struct{}
	exited  chan struct{}
}

func (db *observedPointReader) Get(ctx context.Context, key []byte) ([]byte, error) {
	close(db.entered)
	defer close(db.exited)
	return db.fakeDB.Get(ctx, key)
}

func TestPointReadRunsInWaitWithIssueTimeView(t *testing.T) {
	for _, dependent := range []bool{false, true} {
		name := "unmodified"
		if dependent {
			name = "dependent"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				db := &observedPointReader{
					recordingReader: &recordingReader{fakeDB: newFakeDB([][2]string{{"a", "db"}}, false)},
					entered:         make(chan struct{}),
					exited:          make(chan struct{}),
				}
				r := newRyw()
				defer r.Cancel()
				want := "db"
				if dependent {
					r.AtomicOp([]byte("a"), []byte("-first"), MutationAppendIfFits, 0, false)
					want += "-first"
				}
				key := []byte("a")
				value, wait, err := r.Get(db, key, false)
				if err != nil || wait == nil || value != nil || db.starts != 1 {
					t.Fatalf("issue: value %q wait %v error %v starts %d", value, wait != nil, err, db.starts)
				}
				synctest.Wait()
				select {
				case <-db.entered:
					t.Fatal("point read started a blocking body before its wait")
				default:
				}
				pending, err := r.PendingReads()
				if pending == nil || err != nil {
					t.Fatalf("issued read not tracked: wait %v error %v", pending != nil, err)
				}
				key[0] = 'z'
				r.Set([]byte("a"), []byte("later"))
				value, err = wait(context.Background())
				eqErr(t, err, nil)
				eqVal(t, value, want, true)
				select {
				case <-db.exited:
				default:
					t.Fatal("wait returned before the blocking body unwound")
				}
				eqErr(t, pending(context.Background()), nil)
				if wait, err := r.PendingReads(); wait != nil || err != nil {
					t.Fatalf("completed read still tracked: wait %v error %v", wait != nil, err)
				}
				value, wait, err = r.Get(db, []byte("a"), false)
				eqErr(t, err, nil)
				if wait != nil {
					t.Fatal("later local value did not complete synchronously")
				}
				eqVal(t, value, "later", true)
				eqPairs(t, readConflicts(r), pair{"a", "a\x00"})
			})
		})
	}
}

func TestDirectReadCancellationDrainsBodyAndTracking(t *testing.T) {
	for _, kind := range []string{"point", "dependent", "native"} {
		t.Run(kind, func(t *testing.T) {
			for _, ending := range []string{"caller", "cancel", "reset", "timeout"} {
				t.Run(ending, func(t *testing.T) {
					for _, beforeWait := range []bool{false, true} {
						name := "during-wait"
						if beforeWait {
							name = "before-wait"
						}
						t.Run(name, func(t *testing.T) {
							synctest.Test(t, func(t *testing.T) {
								held, _ := newFakeDB(nil, false).gated(t)
								db := &observedPointReader{
									recordingReader: &recordingReader{fakeDB: held},
									entered:         make(chan struct{}),
									exited:          make(chan struct{}),
								}
								r := newRyw()
								defer r.Cancel()
								if kind == "dependent" {
									r.AtomicOp([]byte("a"), []byte("suffix"), MutationAppendIfFits, 0, false)
								}
								var wait func(context.Context) ([]byte, error)
								var err error
								if kind == "native" {
									_, wait, err = NativeRead(r, func(RywOptions, func([]byte, []byte)) ([]byte, func(context.Context) ([]byte, error), error) {
										db.StartReadVersion()
										return nil, func(ctx context.Context) ([]byte, error) { return db.Get(ctx, []byte("a")) }, nil
									})
								} else {
									_, wait, err = r.Get(db, []byte("a"), false)
								}
								if err != nil || wait == nil {
									t.Fatalf("issue: wait %v error %v", wait != nil, err)
								}
								tracker := r.reading
								ctx, cancel := context.WithCancel(context.Background())
								defer cancel()
								var want error = fdberr.TransactionCancelled
								end := func() {
									switch ending {
									case "caller":
										want = context.Canceled
										cancel()
									case "cancel":
										r.Cancel()
									case "reset":
										r.Reset()
									case "timeout":
										want = fdberr.TransactionTimedOut
										r.Abort(fdberr.TransactionTimedOut)
									}
								}
								if beforeWait {
									end()
								}
								read := capture([]byte(nil), func(context.Context) ([]byte, error) {
									return wait(ctx)
								}, nil)
								if !beforeWait {
									<-db.entered
									end()
								}
								_, err = await_(t, read)
								eqErr(t, err, want)
								select {
								case <-db.exited:
								default:
									t.Fatal("cancellation returned before the native body unwound")
								}
								tracker.mu.Lock()
								running, recorded := tracker.running, tracker.firstErr
								tracker.mu.Unlock()
								if running != 0 {
									t.Fatalf("cancelled read left %d tracked operations", running)
								}
								if ending == "caller" {
									eqErr(t, recorded, context.Canceled)
									_, _, err = r.Commit()
									eqErr(t, err, context.Canceled)
								} else {
									eqErr(t, recorded, nil)
								}
							})
						})
					}
				})
			}
		})
	}
}

func TestWatchSetupFailureDoesNotFailCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db, fail := newFakeDB(nil, false).gated(t)
		r := newRyw()
		defer r.Cancel()
		watch := capture(r.Watch(&failingTrackingReader{db}, []byte("w")))
		commit := capture(r.Commit())
		synctest.Wait()
		if commit.IsReady() {
			t.Fatal("commit did not wait for watch setup")
		}
		fail()
		synctest.Wait()
		if !watch.IsReady() || !commit.IsReady() {
			t.Fatal("watch setup failure did not release commit")
		}
		_, err := await_(t, watch)
		eqErr(t, err, fdberr.AccessedUnreadable)
		must(t, commit)
	})
}
