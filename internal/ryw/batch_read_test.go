package ryw

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/options"
)

type batchReadDB struct {
	*fakeDB
	starts, scalarCalls, batchCalls int
	keys                            [][]byte
	errors                          map[string]error
	entered, exited                 chan struct{}
}

func (db *batchReadDB) StartReadVersion() { db.starts++ }

func (db *batchReadDB) Get(ctx context.Context, key []byte) ([]byte, error) {
	db.scalarCalls++
	return db.fakeDB.Get(ctx, key)
}

func (db *batchReadDB) GetMany(ctx context.Context, keys [][]byte) []kv.ValueResult {
	db.batchCalls++
	if db.entered != nil {
		close(db.entered)
		defer close(db.exited)
	}
	results := make([]kv.ValueResult, len(keys))
	for i, key := range keys {
		db.keys = append(db.keys, bytes.Clone(key))
		value, err := db.fakeDB.Get(ctx, key)
		if err == nil {
			err = db.errors[string(key)]
		}
		results[i] = kv.ValueResult{Value: value, Err: err}
	}
	return results
}

func TestGetManyBatchesOnlyUnresolvedReads(t *testing.T) {
	db := &batchReadDB{fakeDB: newFakeDB([][2]string{{"a", "stored"}, {"empty", ""}, {"cached", "cached"}, {"clear", "old"}}, false)}
	r := newRyw()
	defer r.Cancel()
	must(t, capture(r.Get(db, []byte("cached"), false)))
	r.Set([]byte("local"), []byte("local"))
	r.Clear([]byte("clear"))
	keys := [][]byte{[]byte("a"), []byte("missing"), []byte("empty"), []byte("a"), []byte("local"), []byte("cached"), []byte("clear")}
	values, wait, err := r.GetMany(db, keys, false)
	if err != nil || wait == nil || values != nil || db.starts != 5 || db.batchCalls != 0 {
		t.Fatalf("capture: values %q, pending %v, error %v, starts %d, batches %d", values, wait != nil, err, db.starts, db.batchCalls)
	}
	pending, err := r.PendingReads()
	if pending == nil || err != nil {
		t.Fatalf("captured reads not tracked: pending %v, error %v", pending != nil, err)
	}
	values, err = wait(t.Context())
	eqErr(t, err, nil)
	if len(values) != len(keys) || string(values[0]) != "stored" || values[1] != nil || values[2] == nil || len(values[2]) != 0 || string(values[3]) != "stored" || string(values[4]) != "local" || string(values[5]) != "cached" || values[6] != nil {
		t.Fatalf("batch values lost order, absence, empty value, or local view: %q", values)
	}
	if db.batchCalls != 1 || db.scalarCalls != 1 || len(db.keys) != 4 {
		t.Fatalf("dispatch: batch calls %d, scalar calls %d, keys %q", db.batchCalls, db.scalarCalls, db.keys)
	}
	for i := range db.keys {
		if !bytes.Equal(db.keys[i], keys[i]) {
			t.Fatalf("batch key %d = %q; want %q", i, db.keys[i], keys[i])
		}
	}
	eqErr(t, pending(t.Context()), nil)
	values, wait, err = r.GetMany(db, keys, false)
	if err != nil || wait != nil || len(values) != len(keys) || db.starts != 5 || db.batchCalls != 1 {
		t.Fatalf("cached batch: values %q, pending %v, error %v, starts %d, batches %d", values, wait != nil, err, db.starts, db.batchCalls)
	}
	eqPairs(t, readConflicts(r), pair{"a", "a\x00"}, pair{"cached", "cached\x00"}, pair{"empty", "empty\x00"}, pair{"missing", "missing\x00"})
}

func TestGetManyPinsLocalWritesAndDependentDuplicates(t *testing.T) {
	db := &batchReadDB{fakeDB: newFakeDB([][2]string{{"a", "stored"}, {"dependent", "base"}}, false)}
	r := newRyw()
	defer r.Cancel()
	r.Set([]byte("local"), []byte("before"))
	r.AtomicOp([]byte("dependent"), []byte("-first"), MutationAppendIfFits, AtomicOptions{})
	key := []byte("a")
	_, wait, err := r.GetMany(db, [][]byte{key, []byte("dependent"), []byte("local"), []byte("dependent")}, false)
	if err != nil || wait == nil || db.starts != 3 || db.batchCalls != 0 {
		t.Fatalf("capture: pending %v, error %v, starts %d, batches %d", wait != nil, err, db.starts, db.batchCalls)
	}
	key[0] = 'z'
	r.Set([]byte("a"), []byte("later"))
	r.Set([]byte("local"), []byte("after"))
	r.AtomicOp([]byte("dependent"), []byte("-later"), MutationAppendIfFits, AtomicOptions{})
	values, err := wait(t.Context())
	eqErr(t, err, nil)
	if len(values) != 4 || string(values[0]) != "stored" || string(values[1]) != "base-first" || string(values[2]) != "before" || string(values[3]) != "base-first" {
		t.Fatalf("issue-time batch = %q", values)
	}
	eqVal(t, must(t, capture(r.Get(db, []byte("a"), false))), "later", true)
	eqVal(t, must(t, capture(r.Get(db, []byte("local"), false))), "after", true)
	eqVal(t, must(t, capture(r.Get(db, []byte("dependent"), false))), "base-first-later", true)
	if db.batchCalls != 1 || db.scalarCalls != 0 {
		t.Fatalf("dispatch: batches %d, scalar reads %d", db.batchCalls, db.scalarCalls)
	}
	eqPairs(t, readConflicts(r), pair{"a", "a\x00"}, pair{"dependent", "dependent\x00"})
}

func TestGetManySnapshotWithoutRYW(t *testing.T) {
	db := &batchReadDB{fakeDB: newFakeDB([][2]string{{"a", "stored"}, {"dependent", "base"}}, false)}
	r := New(NewRywOptions(0))
	defer r.Cancel()
	r.Set([]byte("a"), []byte("local"))
	r.AtomicOp([]byte("dependent"), []byte("-local"), MutationAppendIfFits, AtomicOptions{})
	values := must(t, capture(r.GetMany(db, [][]byte{[]byte("a"), []byte("dependent")}, true)))
	if len(values) != 2 || string(values[0]) != "stored" || string(values[1]) != "base" || db.batchCalls != 1 || db.scalarCalls != 0 {
		t.Fatalf("snapshot without RYW = %q, batches %d, scalar reads %d", values, db.batchCalls, db.scalarCalls)
	}
	eqPairs(t, readConflicts(r))
	eqVal(t, must(t, capture(r.Get(db, []byte("a"), false))), "local", true)
	eqVal(t, must(t, capture(r.Get(db, []byte("dependent"), false))), "base-local", true)
}

func TestGetManySettlesAllPlansAndSelectsInputError(t *testing.T) {
	for _, test := range []struct {
		name   string
		keys   [][]byte
		errors map[string]error
		want   error
	}{
		{"read-before-validation", [][]byte{[]byte("a"), []byte("\xff/system"), []byte("b")}, map[string]error{"a": fdberr.TransactionTooOld, "b": fdberr.FutureVersion}, fdberr.TransactionTooOld},
		{"validation-before-read", [][]byte{[]byte("a"), []byte("\xff/system"), []byte("b")}, map[string]error{"b": fdberr.TransactionTooOld}, fdberr.KeyOutsideLegalRange},
		{"validation-first", [][]byte{[]byte("\xff/system"), []byte("a"), []byte("b")}, map[string]error{"a": fdberr.TransactionTooOld}, fdberr.KeyOutsideLegalRange},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := &batchReadDB{fakeDB: newFakeDB([][2]string{{"a", "stored"}, {"b", "stored"}}, false), errors: test.errors}
			r := newRyw()
			defer r.Cancel()
			_, wait, err := r.GetMany(db, test.keys, false)
			if wait == nil || err != nil {
				t.Fatalf("capture failed without settling pending reads: pending %v, error %v", wait != nil, err)
			}
			values, err := wait(t.Context())
			eqErr(t, err, test.want)
			if values != nil || db.batchCalls != 1 || len(db.keys) != 2 {
				t.Fatalf("failed batch: values %q, batches %d, keys %q", values, db.batchCalls, db.keys)
			}
			r.reading.mu.Lock()
			running, recorded := r.reading.running, r.reading.firstErr
			r.reading.mu.Unlock()
			if running != 0 {
				t.Fatalf("failure left %d pending reads", running)
			}
			eqErr(t, recorded, fdberr.TransactionTooOld)
			_, _, err = r.Commit()
			eqErr(t, err, fdberr.TransactionTooOld)
		})
	}
}

func TestGetManyMetadataAndSystemKeyValidation(t *testing.T) {
	db := &batchReadDB{fakeDB: newFakeDB([][2]string{{string(kv.MetadataVersionKey), "version"}, {"\xff/system", "system"}}, false)}
	r := newRyw()
	defer r.Cancel()
	values := must(t, capture(r.GetMany(db, [][]byte{kv.MetadataVersionKey}, false)))
	if len(values) != 1 || string(values[0]) != "version" {
		t.Fatalf("metadata value = %q", values)
	}
	values, wait, err := r.GetMany(db, [][]byte{[]byte("\xff/system")}, false)
	eqErr(t, err, fdberr.KeyOutsideLegalRange)
	if values != nil || wait != nil || db.batchCalls != 1 {
		t.Fatalf("illegal system read: values %q, pending %v, batches %d", values, wait != nil, db.batchCalls)
	}
	r.SetOption(options.TransactionReadSystemKeys)
	values = must(t, capture(r.GetMany(db, [][]byte{[]byte("\xff/system"), kv.MetadataVersionKey}, true)))
	if len(values) != 2 || string(values[0]) != "system" || string(values[1]) != "version" || db.batchCalls != 2 {
		t.Fatalf("enabled system read = %q, batches %d", values, db.batchCalls)
	}
}

func TestGetManyCancellationDrainsEveryPlan(t *testing.T) {
	for _, ending := range []string{"caller", "cancel", "reset", "timeout"} {
		for _, beforeWait := range []bool{false, true} {
			name := ending + "/during-wait"
			if beforeWait {
				name = ending + "/before-wait"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					held, _ := newFakeDB(nil, false).gated(t)
					db := &batchReadDB{fakeDB: held, entered: make(chan struct{}), exited: make(chan struct{})}
					r := newRyw()
					defer r.Cancel()
					r.AtomicOp([]byte("dependent"), []byte("-local"), MutationAppendIfFits, AtomicOptions{})
					_, wait, err := r.GetMany(db, [][]byte{[]byte("a"), []byte("dependent"), []byte("a")}, false)
					if wait == nil || err != nil {
						t.Fatalf("capture: pending %v, error %v", wait != nil, err)
					}
					tracker := r.reading
					ctx, cancel := context.WithCancel(t.Context())
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
					read := capture([][]byte(nil), func(context.Context) ([][]byte, error) { return wait(ctx) }, nil)
					if !beforeWait {
						<-db.entered
						end()
					}
					values, err := await_(t, read)
					eqErr(t, err, want)
					if values != nil || len(db.keys) != 3 {
						t.Fatalf("cancelled batch: values %q, processed keys %q", values, db.keys)
					}
					select {
					case <-db.exited:
					default:
						t.Fatal("batch returned before its body unwound")
					}
					tracker.mu.Lock()
					running, recorded := tracker.running, tracker.firstErr
					tracker.mu.Unlock()
					if running != 0 {
						t.Fatalf("cancelled batch left %d pending reads", running)
					}
					if ending == "caller" {
						eqErr(t, recorded, context.Canceled)
						_, _, err = r.Commit()
						eqErr(t, err, context.Canceled)
					} else {
						eqErr(t, recorded, nil)
					}
					if ending == "reset" {
						r.Set([]byte("a"), []byte("new attempt"))
						eqVal(t, must(t, capture(r.Get(db, []byte("a"), false))), "new attempt", true)
						plan := must(t, capture(r.Commit()))
						if len(plan.ReadConflicts) != 0 {
							t.Fatalf("old batch polluted reset conflicts: %v", plan.ReadConflicts)
						}
					}
				})
			})
		}
	}
}

// scalarBatchBarrierReader cannot complete any call until all keys entered.
// This catches a serial fallback that would deadlock scalar-only readers.
type scalarBatchBarrierReader struct {
	*fakeDB
	entered chan struct{}
	release chan struct{}
}

func (db *scalarBatchBarrierReader) Get(ctx context.Context, key []byte) ([]byte, error) {
	db.entered <- struct{}{}
	select {
	case <-db.release:
		return db.fakeDB.Get(ctx, key)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestGetManyScalarReaderFallbackCapturesBeforeConcurrentWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := &scalarBatchBarrierReader{fakeDB: newFakeDB([][2]string{{"a", "first"}, {"b", "second"}}, false), entered: make(chan struct{}, 2), release: make(chan struct{})}
		r := newRyw()
		defer r.Cancel()
		r.Set([]byte("local"), []byte("before"))
		_, wait, err := r.GetMany(db, [][]byte{[]byte("a"), []byte("local"), []byte("b")}, false)
		if wait == nil || err != nil {
			t.Fatalf("capture: pending %v, error %v", wait != nil, err)
		}
		r.Set([]byte("local"), []byte("after"))
		read := capture([][]byte(nil), wait, nil)
		<-db.entered
		<-db.entered
		close(db.release)
		values := must(t, read)
		if len(values) != 3 || string(values[0]) != "first" || string(values[1]) != "before" || string(values[2]) != "second" {
			t.Fatalf("scalar fallback values = %q", values)
		}
		if pending, err := r.PendingReads(); pending != nil || err != nil {
			t.Fatalf("scalar fallback left pending reads: pending %v, error %v", pending != nil, err)
		}
	})
}
