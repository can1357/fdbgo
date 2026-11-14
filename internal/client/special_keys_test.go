package client

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/options"
	"github.com/can1357/fdbgo/internal/ryw"
)

type fakeSource struct {
	reads []kv.KeyRange
}

func (f *fakeSource) readConflictRows(b, e []byte) ([]kv.KeyValue, error) {
	return coalescedRows(readConflictRangeBegin, f.reads, b, e), nil
}
func (f *fakeSource) writeConflictRows(b, e []byte) []kv.KeyValue { return nil }
func (f *fakeSource) conflictingRanges() ([]kv.KeyRange, bool)    { return nil, false }
func (f *fakeSource) connectionString() string                    { return "a:b@127.0.0.1:4500" }
func (f *fakeSource) statusJSON() ([]byte, error)                 { return nil, nil }
func (f *fakeSource) clusterFilePath() (string, bool)             { return "/tmp/fdb.cluster", true }
func (f *fakeSource) clusterID() (string, error)                  { return "0123", nil }

func sel(k string) kv.KeySelector { return kv.FirstGreaterOrEqual([]byte(k)) }

func TestSpecialKeyspaceSentinelGet(t *testing.T) {
	value, err := specialGet(&fakeSource{}, false, []byte("\xff\xff"))
	if err != nil || value != nil {
		t.Fatalf("sentinel get = %q, %v; want absence", value, err)
	}
}

func TestConflictRangesAreCoalescedBoundaries(t *testing.T) {
	f := &fakeSource{reads: []kv.KeyRange{
		{Begin: []byte("a"), End: []byte("c")},
		{Begin: []byte("b"), End: []byte("d")},
		{Begin: []byte("x"), End: []byte("y")},
	}}
	r, err := specialGetRange(f, false, sel(readConflictRangeBegin), sel(readConflictRangeEnd), kv.DefaultLimits(), false)
	if err != nil {
		t.Fatal(err)
	}
	want := [][2]string{{"a", "1"}, {"d", "0"}, {"x", "1"}, {"y", "0"}}
	if len(r.Data) != len(want) {
		t.Fatalf("got %d rows, want %d", len(r.Data), len(want))
	}
	for i, w := range want {
		k := string(r.Data[i].Key[len(readConflictRangeBegin):])
		if k != w[0] || string(r.Data[i].Value) != w[1] {
			t.Errorf("row %d = (%q, %q), want %q", i, k, r.Data[i].Value, w)
		}
	}
	rev, err := specialGetRange(f, false, sel(readConflictRangeBegin), sel(readConflictRangeEnd), kv.NewLimits(1, kv.Unlimited), true)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(rev.Data[0].Key[len(readConflictRangeBegin):]); got != "y" || !rev.More {
		t.Errorf("reverse first = %q more=%v, want y more=true", got, rev.More)
	}
}

func TestModuleErrorsMatchFlow(t *testing.T) {
	f := &fakeSource{}
	_, err := specialGetRange(f, false, sel("\xff\xff/transaction/"), sel("\xff\xff/zzz"), kv.DefaultLimits(), false)
	if !errors.Is(err, fdberr.SpecialKeysCrossModuleRead) {
		t.Errorf("cross module: %v", err)
	}
	if _, err := specialGet(f, false, []byte("\xff\xff/nope")); !errors.Is(err, fdberr.SpecialKeysNoModuleFound) {
		t.Errorf("no module: %v", err)
	}
	v, err := specialGet(f, false, []byte(connectionStringKey))
	if err != nil || string(v) != "a:b@127.0.0.1:4500" {
		t.Errorf("connection string = %q, %v", v, err)
	}
	v, err = specialGet(f, true, []byte("\xff\xff/nope"))
	if err != nil || v != nil {
		t.Errorf("relaxed unknown = %q, %v", v, err)
	}
}

// startPendingNativeRead holds a real tracked native-read completion until
// released, without involving the network or relying on scheduling delays.
func startPendingNativeRead(t *testing.T, tr *Txn, result error) (chan<- struct{}, <-chan error) {
	t.Helper()
	if !tr.ryw.Options().ReadYourWritesDisabled {
		if err := tr.SetOption(options.TransactionReadYourWritesDisable, nil); err != nil {
			t.Fatal(err)
		}
	}
	attempt := tr.ryw.ResetSignal()
	release, started := make(chan struct{}), make(chan struct{})
	_, wait, err := ryw.NativeRead(tr.ryw, func(ryw.RywOptions, func([]byte, []byte)) (struct{}, func(ctx context.Context) (struct{}, error), error) {
		return struct{}{}, func(ctx context.Context) (struct{}, error) {
			close(started)
			select {
			case <-release:
			case <-attempt.Done():
				return struct{}{}, attempt.Err()
			}
			if result != nil {
				return struct{}{}, result
			}
			return struct{}{}, tr.ryw.AddNativeReadConflictForAttempt(attempt, []byte("a"), []byte("b"))
		}, nil
	})
	if err != nil || wait == nil {
		t.Fatalf("start native read: pending=%v, %v", wait != nil, err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := wait(context.Background())
		done <- err
	}()
	<-started
	return release, done
}

func TestGetManySpecialReadsResumeAfterCapturedReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := localTxn(t)
		release, done := startPendingNativeRead(t, tr, nil)
		keys := [][]byte{[]byte(readConflictRangeBegin + "a"), []byte("\xff\xff"), []byte(readConflictRangeBegin + "b")}
		_, wait, err := tr.GetMany(keys, false)
		if wait == nil || err != nil {
			t.Fatalf("special batch capture = pending=%v, %v", wait != nil, err)
		}
		result := make(chan [][]byte, 1)
		failed := make(chan error, 1)
		go func() {
			values, err := wait(t.Context())
			result <- values
			failed <- err
		}()
		synctest.Wait()
		select {
		case values := <-result:
			t.Fatalf("special batch skipped its captured reads: %q", values)
		default:
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		values := <-result
		if err := <-failed; err != nil {
			t.Fatal(err)
		}
		if len(values) != 3 || string(values[0]) != "1" || values[1] != nil || string(values[2]) != "0" {
			t.Fatalf("special batch results = %q", values)
		}
	})
}

func TestSpecialReadConflictRangesWaitForPendingReads(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{"success", nil},
		{"failure", fdberr.TransactionTooOld},
	} {
		t.Run(test.name, func(t *testing.T) {
			tr := localTxn(t)
			release, done := startPendingNativeRead(t, tr, test.err)
			_, wait, err := tr.GetRange(sel(readConflictRangeBegin), sel(readConflictRangeEnd), RangeOpts{Mode: modeWantAll}, false)
			if err != nil || wait == nil {
				t.Fatalf("conflict read before completion: pending=%v, %v", wait != nil, err)
			}
			close(release)
			if err := <-done; !errors.Is(err, test.err) {
				t.Fatalf("native read = %v; want %v", err, test.err)
			}
			rows, err := wait(context.Background())
			if !errors.Is(err, test.err) {
				t.Fatalf("conflict read = %v; want %v", err, test.err)
			}
			if test.err == nil {
				if len(rows.Data) != 2 || string(rows.Data[0].Key) != readConflictRangeBegin+"a" ||
					string(rows.Data[0].Value) != "1" || string(rows.Data[1].Key) != readConflictRangeBegin+"b" ||
					string(rows.Data[1].Value) != "0" {
					t.Fatalf("conflicts after pending read = %q", rows.Data)
				}
			}
		})
	}
}

func TestSpecialReadConflictRangesIgnoreLaterReadsOnResume(t *testing.T) {
	for _, laterFails := range []bool{false, true} {
		name := "later-pending"
		if laterFails {
			name = "later-failure"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				tr := localTxn(t)
				release, done := startPendingNativeRead(t, tr, nil)
				_, wait, err := tr.GetRange(sel(readConflictRangeBegin), sel(readConflictRangeEnd), RangeOpts{Mode: modeWantAll}, false)
				if err != nil || wait == nil {
					t.Fatalf("conflict read: pending=%v, %v", wait != nil, err)
				}
				var laterErr error
				if laterFails {
					laterErr = fdberr.TransactionTooOld
				}
				laterRelease, laterDone := startPendingNativeRead(t, tr, laterErr)
				if laterFails {
					close(laterRelease)
					if err := <-laterDone; !errors.Is(err, laterErr) {
						t.Fatal(err)
					}
				} else {
					defer func() {
						close(laterRelease)
						<-laterDone
					}()
				}
				close(release)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				result := make(chan error, 1)
				go func() {
					rows, err := wait(context.Background())
					if err == nil && (len(rows.Data) != 2 ||
						string(rows.Data[0].Key) != readConflictRangeBegin+"a" ||
						string(rows.Data[1].Key) != readConflictRangeBegin+"b") {
						err = errors.New("conflict read lost the captured read's boundaries")
					}
					result <- err
				}()
				synctest.Wait()
				select {
				case err := <-result:
					if err != nil {
						t.Fatalf("resumed conflict read included a later failure: %v", err)
					}
				default:
					t.Fatal("resumed conflict read waited for a later read")
				}
			})
		})
	}
}

func TestSpecialReadConflictRangesFailBeforeOtherReadsFinish(t *testing.T) {
	tr := localTxn(t)
	release, done := startPendingNativeRead(t, tr, fdberr.TransactionTooOld)
	_, otherDone := startPendingNativeRead(t, tr, nil)
	close(release)
	if err := <-done; !errors.Is(err, fdberr.TransactionTooOld) {
		t.Fatal(err)
	}
	_, wait, err := tr.Get([]byte(readConflictRangeBegin+"a"), false)
	if wait != nil || !errors.Is(err, fdberr.TransactionTooOld) {
		t.Fatalf("conflict read with failed and pending reads: pending=%v, %v", wait != nil, err)
	}
	tr.Cancel()
	<-otherDone
}

func TestSpecialRangeNormalizesOversizedSelectors(t *testing.T) {
	tr := localTxn(t)
	if err := tr.SetOption(options.TransactionReadSystemKeys, nil); err != nil {
		t.Fatal(err)
	}
	key := append([]byte{0xff}, bytes.Repeat([]byte("k"), kv.SystemKeySizeLimit-len(readConflictRangeBegin))...)
	if err := tr.AddConflictRange(key, kv.KeyAfter(key), options.ConflictRangeTypeRead); err != nil {
		t.Fatal(err)
	}
	boundary := append([]byte(readConflictRangeBegin), key...)
	oversized := append(bytes.Clone(boundary), []byte("ignored suffix")...)
	rows, wait, err := tr.GetRange(kv.FirstGreaterOrEqual(oversized), sel(readConflictRangeEnd), RangeOpts{Mode: modeWantAll}, false)
	if err != nil || wait != nil {
		t.Fatalf("special selector: pending=%v, %v", wait != nil, err)
	}
	if len(rows.Data) != 2 || !bytes.Equal(rows.Data[0].Key, boundary) ||
		!bytes.Equal(rows.Data[1].Key, kv.KeyAfter(boundary)) {
		t.Fatalf("normalized special range returned %d rows; want both conflict boundaries", len(rows.Data))
	}
}

func TestSpecialRangeRejectsEndOutsideSpecialKeyspace(t *testing.T) {
	for _, disableRYW := range []bool{false, true} {
		for _, relaxed := range []bool{false, true} {
			tr := localTxn(t)
			if disableRYW {
				if err := tr.SetOption(options.TransactionReadYourWritesDisable, nil); err != nil {
					t.Fatal(err)
				}
			}
			if relaxed {
				if err := tr.SetOption(options.TransactionSpecialKeySpaceRelaxed, nil); err != nil {
					t.Fatal(err)
				}
			}
			_, wait, err := tr.GetRange(sel(connectionStringKey), sel("z"), RangeOpts{Mode: modeWantAll}, false)
			if wait != nil || !errors.Is(err, fdberr.KeyOutsideLegalRange) {
				t.Fatalf("disabled=%v relaxed=%v: pending=%v, %v; want key_outside_legal_range",
					disableRYW, relaxed, wait != nil, err)
			}
		}
	}
}

func TestRangeOptsLimits(t *testing.T) {
	cases := []struct {
		o    RangeOpts
		want kv.Limits
		err  error
	}{
		{RangeOpts{Mode: modeExact}, kv.Limits{}, fdberr.ExactModeWithoutLimits},
		{RangeOpts{Limit: 10, Mode: modeExact}, kv.NewLimits(10, kv.Unlimited), nil},
		{RangeOpts{Mode: modeWantAll}, kv.NewLimits(kv.Unlimited, 80000), nil},
		{RangeOpts{Mode: modeIterator, Iteration: 1}, kv.NewLimits(kv.Unlimited, 4096), nil},
		{RangeOpts{Mode: modeIterator, Iteration: 3}, kv.NewLimits(kv.Unlimited, 9216), nil},
		{RangeOpts{Mode: modeIterator, Iteration: 100}, kv.NewLimits(kv.Unlimited, 120000), nil},
		{RangeOpts{Mode: modeIterator, Iteration: 0}, kv.Limits{}, fdberr.ClientInvalidOperation},
		{RangeOpts{Mode: 5}, kv.Limits{}, fdberr.ClientInvalidOperation},
		{RangeOpts{Mode: 1, TargetBytes: 100}, kv.NewLimits(kv.Unlimited, 100), nil},
		{RangeOpts{Mode: 1, TargetBytes: 1000}, kv.NewLimits(kv.Unlimited, 256), nil},
		{RangeOpts{Mode: modeExact, TargetBytes: 1000}, kv.NewLimits(kv.Unlimited, 1000), nil},
	}
	for _, c := range cases {
		got, err := c.o.limits()
		if !errors.Is(err, c.err) || (err == nil && got != c.want) {
			t.Errorf("%+v: got %+v, %v; want %+v, %v", c.o, got, err, c.want, c.err)
		}
	}
}
