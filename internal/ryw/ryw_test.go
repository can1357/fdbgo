package ryw

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/options"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestReadsSeeOwnWrites(t *testing.T) {
	db := newFakeDB([][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}}, false)
	r := newRyw()
	if r.Set([]byte("b"), []byte("20")) || r.Clear([]byte("c")) || r.Set([]byte("d"), []byte("4")) {
		t.Fatal("unexpected special key route")
	}
	eqVal(t, must(t, capture(r.Get(db, []byte("b"), false))), "20", true)
	eqVal(t, must(t, capture(r.Get(db, []byte("c"), false))), "", false)
	eqVal(t, must(t, capture(r.Get(db, []byte("a"), false))), "1", true)
	res := must(t, capture(r.GetRange(db, fgoe(""), fgoe("z"), kv.DefaultLimits(), false, false)))
	eqKeys(t, res.Data, "a", "b", "d")
	if res.More {
		t.Fatal("more")
	}
	res = must(t, capture(r.GetRange(db, fgoe(""), fgoe("z"), kv.DefaultLimits(), false, true)))
	eqKeys(t, res.Data, "d", "b", "a")
	if k := must(t, capture(r.GetKey(db, kv.FirstGreaterThan([]byte("b")), false))); string(k) != "d" {
		t.Fatalf("getKey %q", k)
	}
	if k := must(t, capture(r.GetKey(db, kv.LastLessThan([]byte("d")), false))); string(k) != "b" {
		t.Fatalf("getKey %q", k)
	}
}

func TestPointReadConflicts(t *testing.T) {
	db := newFakeDB([][2]string{{"a", "1"}}, false)
	r := newRyw()
	must(t, capture(r.Get(db, []byte("a"), false)))
	must(t, capture(r.Get(db, []byte("s"), true)))
	r.Set([]byte("w"), []byte("x"))
	must(t, capture(r.Get(db, []byte("w"), false)))
	eqPairs(t, readConflicts(r), pair{"a", "a\x00"})

	r.AtomicOp([]byte("n"), []byte{1}, MutationAddValue, AtomicOptions{})
	must(t, capture(r.Get(db, []byte("n"), false)))
	eqPairs(t, readConflicts(r), pair{"a", "a\x00"}, pair{"n", "n\x00"})
}

func TestReadConflictUnionReporting(t *testing.T) {
	r := newRyw()
	r.SetOption(options.TransactionAccessSystemKeys)
	var size int64
	add := func(begin, end string) {
		t.Helper()
		if err := r.AddConflictRange([]byte(begin), []byte(end), false); err != nil {
			t.Fatal(err)
		}
		if begin != end {
			size += int64(len(begin)+len(end)) + sizeofKeyRangeRef
		}
		if got := r.ApproximateSize(); got != size {
			t.Fatalf("approximate size %d, want %d", got, size)
		}
	}
	report := func(begin, end string, want ...[2]string) {
		t.Helper()
		got := r.ReadConflictRanges(withPrefix(ReadConflictRangePrefix, []byte(begin)),
			withPrefix(ReadConflictRangePrefix, []byte(end)))
		if len(got) != len(want) {
			t.Fatalf("boundaries %q, want %q", got, want)
		}
		for i, w := range want {
			if !bytes.Equal(got[i].Key, withPrefix(ReadConflictRangePrefix, []byte(w[0]))) ||
				string(got[i].Value) != w[1] {
				t.Fatalf("boundary %d: %q, want %q", i, got[i], w)
			}
		}
	}
	// Unsorted, touching and overlapping ranges, then duplicates across
	// buffer growth and repeated reporting.
	for range 20 {
		for _, kr := range []pair{{"m", "p"}, {"a", "c"}, {"e", "g"}, {"c", "e"}, {"b", "d"}} {
			add(kr.b, kr.e)
		}
	}
	eqPairs(t, readConflicts(r), pair{"a", "g"}, pair{"m", "p"})
	report("b", "p", [2]string{"g", "0"}, [2]string{"m", "1"})
	report("g", "m", [2]string{"g", "0"})
	report("m", "m")
	add("", "a")
	add("g", "m")
	add("\xff/a", "\xff\xff")
	add("x", "x")
	report("", "\xff\xff", [2]string{"", "1"}, [2]string{"p", "0"}, [2]string{"\xff/a", "1"})
	r.Set([]byte("h"), []byte("v")) // A later write cannot erase an earlier read conflict.
	plan := must(t, capture(r.Commit()))
	eqPairs(t, rangesOf(plan.ReadConflicts), pair{"", "p"}, pair{"\xff/a", "\xff\xff"})
	report("", "\xff\xff", [2]string{"", "1"}, [2]string{"p", "0"}, [2]string{"\xff/a", "1"})
	r.Reset()
	report("", "\xff\xff")
	if r.ApproximateSize() != 0 {
		t.Fatal("reset retained approximate size")
	}
}

func TestRangeReadConflicts(t *testing.T) {
	db := newFakeDB([][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}}, false)

	r := newRyw()
	must(t, capture(r.GetRange(db, fgoe("a"), fgoe("c"), kv.DefaultLimits(), false, false)))
	eqPairs(t, readConflicts(r), pair{"a", "c"})

	r = newRyw()
	res := must(t, capture(r.GetRange(db, fgoe("a"), fgoe("z"), kv.NewLimits(1, kv.Unlimited), false, false)))
	if !res.More {
		t.Fatal("expected more")
	}
	eqPairs(t, readConflicts(r), pair{"a", "a\x00"})

	r = newRyw()
	res = must(t, capture(r.GetRange(db, fgoe("a"), fgoe("z"), kv.NewLimits(1, kv.Unlimited), false, true)))
	eqKeys(t, res.Data, "c")
	eqPairs(t, readConflicts(r), pair{"c", "z"})

	r = newRyw()
	if k := must(t, capture(r.GetKey(db, fgoe("bb"), false))); string(k) != "c" {
		t.Fatalf("getKey %q", k)
	}
	eqPairs(t, readConflicts(r), pair{"bb", "c\x00"})

	r = newRyw()
	if k := must(t, capture(r.GetKey(db, kv.LastLessThan([]byte("b")), false))); string(k) != "a" {
		t.Fatalf("getKey %q", k)
	}
	eqPairs(t, readConflicts(r), pair{"a", "b"})

	r = newRyw()
	if k := must(t, capture(r.GetKey(db, kv.LastLessOrEqual([]byte("b")), false))); string(k) != "b" {
		t.Fatalf("getKey %q", k)
	}
	eqPairs(t, readConflicts(r), pair{"b", "b\x00"})

	r = newRyw()
	r.Set([]byte("b"), []byte("x"))
	must(t, capture(r.GetRange(db, fgoe("a"), fgoe("c"), kv.DefaultLimits(), false, false)))
	eqPairs(t, readConflicts(r), pair{"a", "b"}, pair{"b\x00", "c"})
}

func mut(kind uint8, p1, p2 string) wire.Mutation {
	return wire.Mutation{Kind: kind, Param1: []byte(p1), Param2: []byte(p2)}
}

func eqMutations(t *testing.T, got []wire.Mutation, want ...wire.Mutation) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("mutations %v, want %v", got, want)
	}
	for i := range got {
		if got[i].Kind != want[i].Kind || !bytes.Equal(got[i].Param1, want[i].Param1) || !bytes.Equal(got[i].Param2, want[i].Param2) {
			t.Fatalf("mutation %d: %v, want %v", i, got[i], want[i])
		}
	}
}

func TestExplicitConflictRangesAndCommitPlan(t *testing.T) {
	db := newFakeDB([][2]string{{"r", "1"}}, false)
	r := newRyw()
	r.Set([]byte("a"), []byte("1"))
	r.ClearRange([]byte("b"), []byte("d"))
	r.Set([]byte("c"), []byte("3"))
	r.AtomicOp([]byte("e"), []byte{1, 0}, MutationAddValue, AtomicOptions{})
	r.AtomicOp([]byte("e"), []byte{2, 0}, MutationAddValue, AtomicOptions{})
	r.AtomicOp([]byte("f"), []byte{7}, MutationMinV2, AtomicOptions{})
	if err := r.AddConflictRange([]byte("x"), []byte("y"), true); err != nil {
		t.Fatal(err)
	}
	if err := r.AddConflictRange([]byte("p"), []byte("q"), false); err != nil {
		t.Fatal(err)
	}
	eqErr(t, r.AddConflictRange([]byte("q"), []byte("p"), false), fdberr.InvertedRange)
	must(t, capture(r.Get(db, []byte("r"), false)))
	plan := must(t, capture(r.Commit()))
	eqMutations(t, plan.Mutations,
		mut(MutationClearRange, "b", "c"),
		mut(MutationClearRange, "c\x00", "d"),
		mut(MutationSetValue, "a", "1"),
		mut(MutationSetValue, "c", "3"),
		mut(MutationAddValue, "e", "\x03\x00"),
		mut(MutationMinV2, "f", "\x07"),
	)
	eqPairs(t, rangesOf(plan.WriteConflicts),
		pair{"a", "a\x00"}, pair{"b", "d"}, pair{"e", "e\x00"}, pair{"f", "f\x00"}, pair{"x", "y"})
	eqPairs(t, rangesOf(plan.ReadConflicts), pair{"p", "q"}, pair{"r", "r\x00"})
	if r.ApproximateSize() <= 0 {
		t.Fatal("approximate size")
	}

	ro := newRyw()
	plan = must(t, capture(ro.Commit()))
	if len(plan.Mutations) != 0 || len(plan.WriteConflicts) != 0 {
		t.Fatal("read-only commit has writes")
	}
}

func TestWriteConflictReportAndNoConflictWrites(t *testing.T) {
	r := newRyw()
	r.SetOption(options.TransactionNextWriteNoWriteConflictRange)
	r.Set([]byte("a"), []byte("1"))
	r.Set([]byte("b"), []byte("1"))
	r.SetOption(options.TransactionNextWriteNoWriteConflictRange)
	r.ClearRange([]byte("a"), []byte("c"))
	end := append(append([]byte{}, WriteConflictRangePrefix...), kv.SpecialKeysBegin...)
	marks := r.WriteConflictRanges(WriteConflictRangePrefix, end, nil, 0, false)
	eqPairs(t, pairsFromMarks(marks, WriteConflictRangePrefix), pair{"b", "b\x00"})
	plan := must(t, capture(r.Commit()))
	eqPairs(t, rangesOf(plan.WriteConflicts), pair{"b", "b\x00"})
	eqMutations(t, plan.Mutations, mut(MutationClearRange, "a", "c"))
}

func versionstampTemplate(prefix string, offset int32) []byte {
	k := append([]byte(prefix), make([]byte, 10)...)
	return binary.LittleEndian.AppendUint32(k, uint32(offset))
}

func TestVersionstampRangesAreUnreadable(t *testing.T) {
	db := newFakeDB([][2]string{{"k", "v"}}, false)
	r := newRyw()
	template := versionstampTemplate("vs_", 3)
	r.Set([]byte("vs_0"), []byte("early"))
	r.AtomicOp(template, []byte("val"), MutationSetVersionstampedKey, AtomicOptions{ReadVersion: 41, HasReadVersion: true})
	eqErr(t, r.DeferredError(), nil)

	_, err := await_(t, capture(r.GetRange(db, fgoe("vs_"), fgoe("vs_\xff"), kv.DefaultLimits(), false, false)))
	eqErr(t, err, fdberr.AccessedUnreadable)
	_, err = await_(t, capture(r.Get(db, []byte("vs_\x01"), false)))
	eqErr(t, err, fdberr.AccessedUnreadable)
	eqVal(t, must(t, capture(r.Get(db, []byte("k"), false))), "v", true)

	value := binary.LittleEndian.AppendUint32([]byte("0123456789ab"), 1)
	r.AtomicOp([]byte("sv"), value, MutationSetVersionstampedValue, AtomicOptions{})
	_, err = await_(t, capture(r.Get(db, []byte("sv"), false)))
	eqErr(t, err, fdberr.AccessedUnreadable)
	r.SetOption(options.TransactionBypassUnreadable)
	eqVal(t, must(t, capture(r.Get(db, []byte("sv"), false))), string(value), true)

	_, err = await_(t, capture(r.Commit()))
	eqErr(t, err, fdberr.AccessedUnreadable)

	r = newRyw()
	r.Set([]byte("vs_0"), []byte("early"))
	r.AtomicOp(template, []byte("val"), MutationSetVersionstampedKey, AtomicOptions{ReadVersion: 41, HasReadVersion: true})
	plan := must(t, capture(r.Commit()))
	placed := binary.BigEndian.AppendUint64([]byte("vs_"), 42)
	placed = binary.LittleEndian.AppendUint32(append(placed, 0, 0), 3)
	eqMutations(t, plan.Mutations,
		mut(MutationSetValue, "vs_0", "early"),
		mut(MutationSetVersionstampedKey, string(placed), "val"))
	eqPairs(t, rangesOf(plan.WriteConflicts), pair{"vs_0", "vs_0\x00"})

	bad := newRyw()
	bad.AtomicOp([]byte("short"), []byte("v"), MutationSetVersionstampedKey, AtomicOptions{})
	eqErr(t, bad.DeferredError(), fdberr.ClientInvalidOperation)
	bad = newRyw()
	bad.AtomicOp(kv.MetadataVersionKey, []byte("x"), MutationSetVersionstampedValue, AtomicOptions{})
	eqErr(t, bad.DeferredError(), fdberr.ClientInvalidOperation)
	ok := newRyw()
	ok.AtomicOp(kv.MetadataVersionKey, MetadataVersionRequiredValue, MutationSetVersionstampedValue, AtomicOptions{})
	eqErr(t, ok.DeferredError(), nil)
}

func TestSnapshotRywDisableReadsTheSnapshot(t *testing.T) {
	db := newFakeDB([][2]string{{"a", "db"}}, false)
	r := newRyw()
	r.Set([]byte("a"), []byte("mine"))
	eqVal(t, must(t, capture(r.Get(db, []byte("a"), true))), "mine", true)
	r.SetOption(options.TransactionSnapshotRYWDisable)
	eqVal(t, must(t, capture(r.Get(db, []byte("a"), true))), "db", true)
	eqVal(t, must(t, capture(r.Get(db, []byte("a"), false))), "mine", true)
	res := must(t, capture(r.GetRange(db, fgoe(""), fgoe("z"), kv.DefaultLimits(), true, false)))
	if len(res.Data) != 1 || string(res.Data[0].Key) != "a" || string(res.Data[0].Value) != "db" {
		t.Fatalf("rows %v", res.Data)
	}
	eqPairs(t, readConflicts(r))
}

func TestReadsArePinnedAtCallTime(t *testing.T) {
	db, release := newFakeDB([][2]string{{"a", "1"}, {"c", "3"}}, false).gated(t)
	r := newRyw()
	rng := capture(r.GetRange(db, fgoe(""), fgoe("z"), kv.DefaultLimits(), false, false))
	point := capture(r.Get(db, []byte("a"), false))
	r.Set([]byte("b"), []byte("2"))
	r.Set([]byte("a"), []byte("10"))
	if rng.IsReady() || point.IsReady() {
		t.Fatal("reads completed before their replies")
	}
	release()
	eqKeys(t, must(t, rng).Data, "a", "c")
	eqVal(t, must(t, point), "1", true)
	eqVal(t, must(t, capture(r.Get(db, []byte("a"), false))), "10", true)
	eqKeys(t, must(t, capture(r.GetRange(db, fgoe(""), fgoe("z"), kv.DefaultLimits(), false, false))).Data, "a", "b", "c")
}

func TestResetAndCancelFailReadsInFlight(t *testing.T) {
	direct := newFakeDB([][2]string{{"a", "1"}}, true)
	held, _ := direct.gated(t)
	r := newRyw()
	read := capture(r.Get(held, []byte("a"), false))
	r.Reset()
	_, err := await_(t, read)
	eqErr(t, err, fdberr.TransactionCancelled)
	eqVal(t, must(t, capture(r.Get(direct, []byte("a"), false))), "1", true)
	if len(readConflicts(r)) != 1 {
		t.Fatal("expected one read conflict")
	}

	read = capture(r.Get(held, []byte("zz"), false))
	r.Cancel()
	_, err = await_(t, read)
	eqErr(t, err, fdberr.TransactionCancelled)
	_, err = await_(t, capture(r.Get(direct, []byte("a"), false)))
	eqErr(t, err, fdberr.TransactionCancelled)
	_, err = await_(t, capture(r.Commit()))
	eqErr(t, err, fdberr.TransactionCancelled)
	r.Reset()
	eqPairs(t, readConflicts(r))
	eqVal(t, must(t, capture(r.Get(direct, []byte("a"), false))), "1", true)
}

func TestDeferredErrorsAndUsedDuringCommit(t *testing.T) {
	db := newFakeDB(nil, false)
	r := newRyw()
	r.Set(bytes.Repeat([]byte("k"), kv.KeySizeLimit+1), []byte("v"))
	r.Set([]byte("fine"), []byte("v"))
	_, err := await_(t, capture(r.Get(db, []byte("fine"), false)))
	eqErr(t, err, fdberr.KeyTooLarge)
	_, err = await_(t, capture(r.Commit()))
	eqErr(t, err, fdberr.KeyTooLarge)
	r.Reset()
	eqVal(t, must(t, capture(r.Get(db, []byte("fine"), false))), "", false)

	r.ClearRange([]byte("b"), []byte("a"))
	eqErr(t, r.DeferredError(), fdberr.InvertedRange)
	r.Reset()
	r.Set([]byte("\xff/x"), []byte("v"))
	eqErr(t, r.DeferredError(), fdberr.KeyOutsideLegalRange)
	r.Reset()
	r.Set(kv.MetadataVersionKey, []byte("v"))
	eqErr(t, r.DeferredError(), fdberr.ClientInvalidOperation)
	r.Reset()
	if !r.Set([]byte("\xff\xff/special"), []byte("v")) {
		t.Fatal("special key not routed")
	}
	_, err = await_(t, capture(r.Get(db, []byte("\xff/x"), false)))
	eqErr(t, err, fdberr.KeyOutsideLegalRange)

	// With no pending reads the plan is ready at once; a write after that ends the attempt, which the
	// caller's race against ResetSignal turns into used_during_commit.
	plan := capture(r.Commit())
	r.Set([]byte("late"), []byte("v"))
	eqErr(t, r.DeferredError(), fdberr.UsedDuringCommit)
	if _, err := await_(t, plan); err != nil {
		eqErr(t, err, fdberr.UsedDuringCommit)
	}
	end := r.ResetSignal()
	select {
	case <-end.Done():
	default:
		t.Fatal("attempt did not end")
	}
	eqErr(t, end.Err(), fdberr.UsedDuringCommit)
	_, err = await_(t, capture(r.Get(db, []byte("a"), false)))
	eqErr(t, err, fdberr.UsedDuringCommit)

	// A commit still waiting for reads fails with used_during_commit.
	r.Reset()
	held, release := db.gated(t)
	_ = capture(r.Get(held, []byte("x"), false))
	plan = capture(r.Commit())
	r.Set([]byte("late"), []byte("v"))
	_, err = await_(t, plan)
	eqErr(t, err, fdberr.UsedDuringCommit)
	release()
}

func TestCommitWaitsForDroppedReads(t *testing.T) {
	db := newFakeDB([][2]string{{"a", "1"}}, true)
	r := newRyw()
	_ = capture(r.Get(db, []byte("a"), false))
	_ = capture(r.GetRange(db, fgoe("m"), fgoe("n"), kv.DefaultLimits(), false, false))
	plan := must(t, capture(r.Commit()))
	eqPairs(t, rangesOf(plan.ReadConflicts), pair{"a", "a\x00"}, pair{"m", "n"})
	if len(plan.Mutations) != 0 || len(plan.WriteConflicts) != 0 {
		t.Fatal("unexpected writes")
	}
}

func TestFailedReadFailsCommit(t *testing.T) {
	db := newFakeDB(nil, false)
	r := newRyw()
	value := binary.LittleEndian.AppendUint32([]byte("0123456789ab"), 1)
	r.AtomicOp([]byte("sv"), value, MutationSetVersionstampedValue, AtomicOptions{})
	_, err := await_(t, capture(r.Get(db, []byte("sv"), false)))
	eqErr(t, err, fdberr.AccessedUnreadable)
	_, err = await_(t, capture(r.Commit()))
	eqErr(t, err, fdberr.AccessedUnreadable)
	eqErr(t, r.CheckReadYourWritesDisable(), fdberr.ClientInvalidOperation)
	eqErr(t, newRyw().CheckReadYourWritesDisable(), nil)
}

func TestCompareAndClearDeletes(t *testing.T) {
	db := newFakeDB([][2]string{{"d", "x"}}, false)
	r := newRyw()
	r.Set([]byte("k"), []byte("x"))
	r.AtomicOp([]byte("k"), []byte("x"), MutationCompareAndClear, AtomicOptions{})
	eqVal(t, must(t, capture(r.Get(db, []byte("k"), false))), "", false)
	r.AtomicOp([]byte("d"), []byte("x"), MutationCompareAndClear, AtomicOptions{})
	eqVal(t, must(t, capture(r.Get(db, []byte("d"), false))), "", false)
	res := must(t, capture(r.GetRange(db, fgoe(""), fgoe("z"), kv.DefaultLimits(), false, false)))
	eqKeys(t, res.Data)
}

func TestWatchesFireOnOwnWrites(t *testing.T) {
	db := newFakeDB([][2]string{{"w", "1"}}, true)
	r := newRyw()
	same := capture(r.Watch(db, []byte("w")))
	changed := capture(r.Watch(db, []byte("w")))
	if wait, err := r.PendingReads(); err != nil {
		t.Fatal(err)
	} else if wait != nil {
		eqErr(t, wait(context.Background()), nil)
	}
	r.Set([]byte("w"), []byte("1"))
	if same.IsReady() {
		t.Fatal("watch fired on an unchanged value")
	}
	r.Set([]byte("w"), []byte("2"))
	must(t, changed)
	must(t, same)
	eqPairs(t, readConflicts(r), pair{"w", "w\x00"})

	early := capture(r.Watch(db, []byte("v")))
	r.AtomicOp([]byte("v"), []byte{1}, MutationAddValue, AtomicOptions{})
	must(t, early)

	held, _ := db.gated(t)
	cancelled := capture(r.Watch(held, []byte("u")))
	r.Reset()
	_, err := await_(t, cancelled)
	eqErr(t, err, fdberr.TransactionCancelled)
	_, err = await_(t, capture(r.Watch(db, []byte("\xff/x"))))
	eqErr(t, err, fdberr.KeyOutsideLegalRange)
}

func TestExtOrderingMatchesMaterializedKeys(t *testing.T) {
	bases := []string{"", "\x00", "a", "a\x00", "\xff", "a\x01"}
	var all []ext
	for _, b := range bases {
		for z := range 3 {
			all = append(all, ext{[]byte(b), z})
		}
	}
	for _, x := range all {
		for _, y := range all {
			xb, yb := x.toBytes(), y.toBytes()
			if x.compare(y) != bytes.Compare(xb, yb) || x.cmpKey(yb) != bytes.Compare(xb, yb) {
				t.Fatalf("%q vs %q", xb, yb)
			}
			after := len(xb)+1 == len(yb) && bytes.HasSuffix(yb, []byte{0}) && bytes.HasPrefix(yb, xb)
			if y.isKeyAfter(x) != after {
				t.Fatalf("%q after %q", yb, xb)
			}
		}
	}
}

func TestCommitPlanOwnsMutationBytes(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprint(native), func(t *testing.T) {
			r := newRyw()
			r.SetOption(options.TransactionUsedDuringCommitProtectionDisable)
			if native {
				r.SetOption(options.TransactionReadYourWritesDisable)
			}
			key, value := []byte("a"), []byte("value")
			r.Set(key, value)
			key[0], value[0] = 'z', 'X'
			key, operand := []byte("n"), []byte{1}
			r.AtomicOp(key, operand, MutationAddValue, AtomicOptions{})
			key[0], operand[0] = 'z', 99
			begin, end := []byte("x"), []byte("y")
			if err := r.AddConflictRange(begin, end, true); err != nil {
				t.Fatal(err)
			}
			begin[0], end[0] = 'b', 'c'
			plan := must(t, capture(r.Commit()))
			r.Set([]byte("a"), []byte("later"))
			r.AtomicOp([]byte("n"), []byte{2}, MutationAddValue, AtomicOptions{})
			r.Reset()
			r.Set([]byte("a"), []byte("reset"))
			eqMutations(t, plan.Mutations,
				mut(MutationSetValue, "a", "value"),
				mut(MutationAddValue, "n", "\x01"))
			eqPairs(t, rangesOf(plan.WriteConflicts),
				pair{"a", "a\x00"}, pair{"n", "n\x00"}, pair{"x", "y"})
		})
	}
}

func TestCoalescedMapMergesNeighbours(t *testing.T) {
	m := newCoalescedMap(false, []byte("\xff\xff"))
	m.insert([]byte("b"), []byte("d"), true)
	m.insert([]byte("d"), []byte("f"), true)
	m.insert([]byte("a"), []byte("b"), true)
	m.insert([]byte("c"), []byte("e"), false)
	var got []string
	m.ranges(func(b, e []byte, v bool) { got = append(got, fmt.Sprintf("%s-%s:%v", b, e, v)) })
	want := []string{"-a:false", "a-c:true", "c-e:false", "e-f:true", "f-\xff\xff:false"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("ranges %q, want %q", got, want)
	}
}

func TestReadYourWritesDisabledGoesNative(t *testing.T) {
	db := newFakeDB([][2]string{{"a", "1"}}, false)
	r := newRyw()
	r.SetOption(options.TransactionReadYourWritesDisable)
	if !r.Options().ReadYourWritesDisabled {
		t.Fatal("option not set")
	}
	r.Set([]byte("b"), []byte("2"))
	r.Clear([]byte("c"))
	r.SetOption(options.TransactionNextWriteNoWriteConflictRange)
	r.ClearRange([]byte("d"), []byte("f"))
	r.AtomicOp([]byte("e"), []byte{1}, MutationAndV2, AtomicOptions{})
	if err := r.AddConflictRange([]byte("r"), []byte("s"), false); err != nil {
		t.Fatal(err)
	}
	if err := r.AddConflictRange([]byte("w"), []byte("x"), true); err != nil {
		t.Fatal(err)
	}
	r.AddNativeReadConflict([]byte("a"), []byte("a\x00"))
	read := capture(NativeRead(r, func(o RywOptions, _ func(b, e []byte)) ([]byte, func(ctx context.Context) ([]byte, error), error) {
		if _, err := ValidateGet(&o, []byte("a")); err != nil {
			return nil, nil, err
		}
		return nil, func(ctx context.Context) ([]byte, error) { return db.Get(ctx, []byte("a")) }, nil
	}))
	eqVal(t, must(t, read), "1", true)
	_, err := await_(t, capture(r.Watch(db, []byte("a"))))
	eqErr(t, err, fdberr.WatchesDisabled)

	eqPairs(t, readConflicts(r), pair{"a", "a\x00"}, pair{"r", "s"})

	plan := must(t, capture(r.Commit()))
	eqMutations(t, plan.Mutations,
		mut(MutationSetValue, "b", "2"),
		mut(MutationClearRange, "c", "c\x00"),
		mut(MutationClearRange, "d", "f"),
		mut(MutationAndV2, "e", "\x01"))
	eqPairs(t, rangesOf(plan.WriteConflicts), pair{"b", "b\x00"}, pair{"c", "c\x00"}, pair{"e", "e\x00"}, pair{"w", "x"})
	eqPairs(t, rangesOf(plan.ReadConflicts), pair{"r", "s"}, pair{"a", "a\x00"})
	eqPairs(t, readConflicts(r), pair{"a", "a\x00"}, pair{"r", "s"})

	late := newRyw()
	late.Set([]byte("k"), []byte("v"))
	late.SetOption(options.TransactionReadYourWritesDisable)
	eqErr(t, late.DeferredError(), fdberr.ClientInvalidOperation)
}

// ---- randomized model test ----

// rng is xorshift64*, deterministic per seed.
type rng uint64

func (r *rng) next() uint64 {
	x := uint64(*r)
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	*r = rng(x)
	return x * 0x2545_f491_4f6c_dd1d
}

func (r *rng) below(n uint64) uint64 { return r.next() % n }

func (r *rng) between(lo, hi int64) int64 { return lo + int64(r.below(uint64(hi-lo))) }

func (r *rng) chance(p float64) bool { return float64(r.next()>>11)/float64(uint64(1)<<53) <= p }

// keyForIndex maps an index to one of 34 base keys each followed
// by one or two zero bytes.
func keyForIndex(idx uint64) []byte {
	k := fmt.Appendf(nil, "%010d", idx/3)
	return append(k, make([]byte, idx%3)...)
}

func randomValue(r *rng) []byte {
	v := make([]byte, r.below(6))
	for i := range v {
		v[i] = byte(r.below(256))
	}
	return v
}

func randomSelector(r *rng) kv.KeySelector {
	return kv.KeySelector{Key: keyForIndex(r.below(100)), OrEqual: r.chance(0.5), Offset: int(r.between(-10, 10))}
}

func randomLimits(r *rng) kv.Limits {
	rows := kv.Unlimited
	if !r.chance(0.5) {
		rows = int(r.between(1, 15))
	}
	byteLimit := kv.Unlimited
	if !r.chance(0.7) {
		byteLimit = int(r.between(1, 200))
	}
	return kv.NewLimits(rows, byteLimit)
}

func sortedRows(model map[string][]byte) []kv.KeyValue {
	rows := make([]kv.KeyValue, 0, len(model))
	for k, v := range model {
		rows = append(rows, kv.KeyValue{Key: []byte(k), Value: v})
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].Key, rows[j].Key) < 0 })
	return rows
}

// expectedRange resolves both selectors over the model, then takes rows until
// the limits are reached.
func expectedRange(model map[string][]byte, begin, end kv.KeySelector, limits kv.Limits, reverse bool) []kv.KeyValue {
	rows := sortedRows(model)
	keys := keysOf(rows)
	begin.RemoveOrEqual()
	end.RemoveOrEqual()
	if limits.IsReached() || (begin.Offset >= end.Offset && bytes.Compare(begin.Key, end.Key) >= 0) {
		return nil
	}
	b, e := resolveIn(keys, begin, kv.SystemKeysBegin), resolveIn(keys, end, kv.SystemKeysBegin)
	if bytes.Compare(b, e) >= 0 {
		return nil
	}
	var in []kv.KeyValue
	for _, row := range rows {
		if bytes.Compare(row.Key, b) >= 0 && bytes.Compare(row.Key, e) < 0 {
			in = append(in, row)
		}
	}
	var out []kv.KeyValue
	for i := range in {
		row := in[i]
		if reverse {
			row = in[len(in)-1-i]
		}
		if limits.IsReached() {
			break
		}
		limits.DecrementOne(&row)
		out = append(out, row)
	}
	return out
}

var modelOps = []uint8{MutationAddValue, MutationAnd, MutationOr, MutationXor, MutationMax, MutationMin,
	MutationByteMin, MutationByteMax, MutationAppendIfFits, MutationAndV2, MutationMinV2}

func rowsEqual(a, b []kv.KeyValue) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].Key, b[i].Key) || !bytes.Equal(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}

func runModel(t *testing.T, seed uint64, async bool) {
	rnd := rng(seed*0x9e37_79b9_7f4a_7c15 | 1)
	data := map[string][]byte{}
	for i := range uint64(100) {
		if rnd.chance(0.4) {
			data[string(keyForIndex(i))] = randomValue(&rnd)
		}
	}
	db := fakeFromMap(data, async)
	model := map[string][]byte{}
	for k, v := range data {
		model[k] = v
	}
	r := newRyw()

	for step := range 300 {
		switch rnd.below(10) {
		case 0, 1, 2:
			k, v := keyForIndex(rnd.below(100)), randomValue(&rnd)
			r.Set(k, v)
			model[string(k)] = v
		case 3:
			k := keyForIndex(rnd.below(100))
			r.Clear(k)
			delete(model, string(k))
		case 4:
			s := rnd.below(100)
			e := s + uint64(rnd.between(1, int64(101-s)))
			bk, ek := keyForIndex(s), keyForIndex(e)
			r.ClearRange(bk, ek)
			for k := range model {
				if k >= string(bk) && k < string(ek) {
					delete(model, k)
				}
			}
		case 5, 6:
			k := keyForIndex(rnd.below(100))
			op := modelOps[rnd.below(uint64(len(modelOps)))]
			param := randomValue(&rnd)
			r.AtomicOp(k, param, op, AtomicOptions{})
			if v := applyAtomic(op, model[string(k)], param); v != nil {
				model[string(k)] = v
			} else {
				delete(model, string(k))
			}
		default:
			snapshot := rnd.chance(0.3)
			switch rnd.below(4) {
			case 0:
				k := keyForIndex(rnd.below(100))
				got := must(t, capture(r.Get(db, k, snapshot)))
				if want := model[string(k)]; !optEqual(got, want) {
					t.Fatalf("seed %d step %d: get %q = %q, want %q", seed, step, k, got, want)
				}
			case 1:
				sel := randomSelector(&rnd)
				got := must(t, capture(r.GetKey(db, sel, snapshot)))
				want := resolveIn(keysOf(sortedRows(model)), sel, kv.SystemKeysBegin)
				if !bytes.Equal(got, want) {
					t.Fatalf("seed %d step %d: getKey %+v = %q, want %q", seed, step, sel, got, want)
				}
			default:
				var begin, end kv.KeySelector
				if rnd.chance(0.5) {
					s := rnd.below(100)
					e := s + rnd.below(101-s)
					begin, end = kv.FirstGreaterOrEqual(keyForIndex(s)), kv.FirstGreaterOrEqual(keyForIndex(e))
				} else {
					begin, end = randomSelector(&rnd), randomSelector(&rnd)
				}
				limits := randomLimits(&rnd)
				reverse := rnd.chance(0.5)
				got := must(t, capture(r.GetRange(db, begin, end, limits, snapshot, reverse)))
				want := expectedRange(model, begin, end, limits, reverse)
				ctx := fmt.Sprintf("seed %d step %d: range %+v..%+v %+v rev=%v", seed, step, begin, end, limits, reverse)
				if limits.HasByteLimit() {
					if len(got.Data) > len(want) || !rowsEqual(got.Data, want[:len(got.Data)]) ||
						(len(got.Data) != len(want) && !got.More) {
						t.Fatalf("%s: got %q, want %q", ctx, keysOf(got.Data), keysOf(want))
					}
				} else if !rowsEqual(got.Data, want) {
					t.Fatalf("%s: got %q, want %q", ctx, keysOf(got.Data), keysOf(want))
				}
			}
		}
	}
	plan := must(t, capture(r.Commit()))
	if len(plan.Mutations) == 0 && len(plan.WriteConflicts) == 0 {
		t.Fatalf("seed %d: empty commit", seed)
	}
}

func TestModelRandomOperations(t *testing.T) {
	for seed := uint64(1); seed <= 40; seed++ {
		runModel(t, seed, false)
	}
}

func TestModelRandomOperationsWithAsyncReads(t *testing.T) {
	for seed := uint64(41); seed <= 60; seed++ {
		runModel(t, seed, true)
	}
}

// recordingReader observes synchronous read-version pinning and controls watch delivery.
type recordingReader struct {
	*fakeDB
	starts int
	watch  chan error
}

func (db *recordingReader) StartReadVersion() { db.starts++ }
func (db *recordingReader) RegisterWatch(_, _ []byte) (<-chan error, func(error), error) {
	var once sync.Once
	return db.watch, func(err error) { once.Do(func() { db.watch <- err }) }, nil
}

func TestBlockingReadPinsVersionAtIssue(t *testing.T) {
	held, release := newFakeDB([][2]string{{"a", "1"}}, false).gated(t)
	db := &recordingReader{fakeDB: held}
	r := newRyw()
	r.Set([]byte("local"), []byte("value"))
	value, wait, err := r.Get(db, []byte("local"), false)
	if err != nil || wait != nil || string(value) != "value" || db.starts != 0 {
		t.Fatalf("local read: value %q wait %v error %v starts %d", value, wait != nil, err, db.starts)
	}
	value, wait, err = r.Get(db, []byte("a"), false)
	read := capture(value, wait, err)
	if err != nil || wait == nil || db.starts != 1 {
		t.Fatalf("snapshot issue: wait %v error %v starts %d", wait != nil, err, db.starts)
	}
	release()
	eqVal(t, must(t, read), "1", true)
	value, wait, err = r.Get(db, []byte("a"), false)
	if err != nil || wait != nil || string(value) != "1" || db.starts != 1 {
		t.Fatalf("cached read: value %q wait %v error %v starts %d", value, wait != nil, err, db.starts)
	}
}

func TestArmedWatchSurvivesReset(t *testing.T) {
	db := &recordingReader{fakeDB: newFakeDB(nil, false), watch: make(chan error, 1)}
	r := newRyw()
	r.Set([]byte("a"), []byte("value"))
	watch := capture(r.Watch(db, []byte("a")))
	if wait, err := r.PendingReads(); err != nil {
		t.Fatal(err)
	} else if wait != nil {
		eqErr(t, wait(context.Background()), nil)
	}
	r.Reset()
	db.watch <- nil
	must(t, watch)
}

func TestDroppedBlockingReadKeepsCommitPending(t *testing.T) {
	db, release := newFakeDB([][2]string{{"a", "1"}}, false).gated(t)
	r := newRyw()
	_ = capture(r.Get(db, []byte("a"), false))
	plan := capture(r.Commit())
	if plan.IsReady() {
		t.Fatal("commit completed before the dropped read")
	}
	release()
	got := must(t, plan)
	eqPairs(t, rangesOf(got.ReadConflicts), pair{"a", "a\x00"})
}

func TestAttemptScopedConflictsRejectReset(t *testing.T) {
	r := newRyw()
	attempt := r.ResetSignal()
	r.Reset()
	eqErr(t, r.AddNativeReadConflictForAttempt(attempt, []byte("a"), []byte("b")), fdberr.TransactionCancelled)
	eqErr(t, r.AddMappedRangeConflictsForAttempt(attempt, fgoe("a"), fgoe("b"), false, RangeExtent{}, nil), fdberr.TransactionCancelled)
	plan := must(t, capture(r.Commit()))
	if len(plan.ReadConflicts) != 0 {
		t.Fatalf("stale conflicts: %v", plan.ReadConflicts)
	}
}
