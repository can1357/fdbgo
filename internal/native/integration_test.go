package native

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/cluster"
	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/ryw"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

// Mutation type codes the tests write.
const (
	mutSetValue   = 0
	mutClearRange = 1
)

func openTestDB(t *testing.T) (*DbContext, []byte) {
	t.Helper()
	path := os.Getenv("FDB_CLUSTER_FILE")
	if path == "" {
		t.Skip("FDB_CLUSTER_FILE not set")
	}
	rec, err := cluster.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	db := NewDbContext(*rec, transport.TLSEnabled())
	prefix := fmt.Appendf(nil, "native_test/%s/%d/", t.Name(), time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		end := append(slices.Clone(prefix), 0xff)
		_ = commitPlan(ctx, db, ryw.CommitPlan{
			Mutations:      []wire.Mutation{{Kind: mutClearRange, Param1: prefix, Param2: end}},
			WriteConflicts: []kv.KeyRange{{Begin: prefix, End: end}},
		})
		db.Close()
	})
	return db, prefix
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// commitPlan commits plan in a fresh transaction, retrying retryable errors.
func commitPlan(ctx context.Context, db *DbContext, plan ryw.CommitPlan) error {
	tr := newNativeTxn(ctx, db, DefaultNativeOptions())
	defer tr.Close()
	for {
		err := tr.Commit(plan)
		if err == nil {
			return nil
		}
		delay, err := tr.OnError(err)
		if err != nil {
			return err
		}
		time.Sleep(delay)
	}
}

func waitWatchResult(ctx context.Context, ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func setPlan(kvs ...[]byte) ryw.CommitPlan {
	var p ryw.CommitPlan
	for i := 0; i+1 < len(kvs); i += 2 {
		p.Mutations = append(p.Mutations, wire.Mutation{Kind: mutSetValue, Param1: kvs[i], Param2: kvs[i+1]})
		p.WriteConflicts = append(p.WriteConflicts, pointRange(kvs[i]))
	}
	return p
}

func k(prefix []byte, s string) []byte { return append(slices.Clone(prefix), s...) }

func TestIntegrationReadVersionAndStatus(t *testing.T) {
	db, _ := openTestDB(t)
	ctx := testCtx(t)
	tr := NewNativeTxn(db, DefaultNativeOptions())
	defer tr.Close()
	rv, err := tr.Shared.ReadVersion(ctx)
	if err != nil || rv.Version <= 0 {
		t.Fatalf("read version %v %v", rv, err)
	}
	if v, ok := tr.Shared.CachedReadVersion(); !ok || v != rv.Version {
		t.Fatal("cached read version")
	}
	// Concurrent requests share batches and see increasing versions.
	futs := make([]*NativeTxn, 50)
	for i := range futs {
		futs[i] = NewNativeTxn(db, DefaultNativeOptions())
		futs[i].Shared.StartReadVersion()
	}
	for _, f := range futs {
		v, err := f.Shared.ReadVersion(ctx)
		if err != nil || v.Version < rv.Version {
			t.Fatalf("batched read version %v %v", v, err)
		}
		f.Close()
	}
	if db.ClusterNow().ClusterID == (wire.UID{}) {
		t.Fatal("cluster id unknown after a read version")
	}
	if _, ok := db.Coordinator(); !ok {
		t.Fatal("no current coordinator")
	}

	doc, err := StatusJSON(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	var st map[string]any
	if err := json.Unmarshal(doc, &st); err != nil {
		t.Fatalf("status is not JSON: %v", err)
	}
	if q, _ := jget(st, "client.coordinators.quorum_reachable"); q != true {
		t.Fatalf("quorum not reachable: %s", doc)
	}
	if _, ok := jget(st, "cluster.processes"); !ok {
		t.Fatalf("no cluster section: %s", doc)
	}
	if v, _ := jget(st, "client.database_status.available"); v != true {
		t.Fatalf("database not available: %s", doc)
	}
}

func TestIntegrationSetGetRange(t *testing.T) {
	db, prefix := openTestDB(t)
	ctx := testCtx(t)
	var kvs [][]byte
	for i := range 20 {
		kvs = append(kvs, k(prefix, fmt.Sprintf("k%02d", i)), fmt.Appendf(nil, "v%d", i))
	}
	kvs = append(kvs, k(prefix, "empty"), []byte{})
	if err := commitPlan(ctx, db, setPlan(kvs...)); err != nil {
		t.Fatal(err)
	}

	tr := NewNativeTxn(db, DefaultNativeOptions())
	defer tr.Close()
	r := tr.Reader()
	v, err := r.Get(ctx, k(prefix, "k03"))
	if err != nil || string(v) != "v3" {
		t.Fatalf("get: %q %v", v, err)
	}
	// Now the location and read version are cached: the hot path.
	for i := range 20 {
		v, err := r.Get(ctx, k(prefix, fmt.Sprintf("k%02d", i)))
		if err != nil || string(v) != fmt.Sprint("v", i) {
			t.Fatalf("get %d: %q %v", i, v, err)
		}
	}
	if v, err := r.Get(ctx, k(prefix, "missing")); err != nil || v != nil {
		t.Fatalf("absent key: %q %v", v, err)
	}
	if v, err := r.Get(ctx, k(prefix, "empty")); err != nil || v == nil || len(v) != 0 {
		t.Fatalf("present empty value: %q (nil=%v) %v", v, v == nil, err)
	}

	end := k(prefix, "l")
	res, err := r.GetRange(ctx, kv.FirstGreaterOrEqual(k(prefix, "k")), kv.FirstGreaterOrEqual(end), kv.DefaultLimits(), false)
	if err != nil || len(res.Data) != 20 || res.More {
		t.Fatalf("range: %d rows more=%v err=%v", len(res.Data), res.More, err)
	}
	res, err = r.GetRange(ctx, kv.FirstGreaterOrEqual(k(prefix, "k")), kv.FirstGreaterOrEqual(end), kv.NewLimits(5, kv.Unlimited), true)
	if err != nil || len(res.Data) != 5 || !res.More || string(res.Data[0].Key) != string(k(prefix, "k19")) {
		t.Fatalf("reverse limited range: %v err=%v", res, err)
	}
	out, err := r.GetRangeOut(ctx, kv.FirstGreaterThan(k(prefix, "k04")), kv.FirstGreaterOrEqual(k(prefix, "k07")), kv.DefaultLimits(), false)
	if err != nil || len(out.Out.Data) != 2 {
		t.Fatalf("range out: %v %v", out, err)
	}
	cr := GetRangeConflict(out.Begin, out.End, &out.Out, func(r *kv.KeyValue) []byte { return r.Key }, false)
	if !bytes.Equal(cr.Begin, kv.KeyAfter(k(prefix, "k04"))) || !bytes.Equal(cr.End, k(prefix, "k07")) {
		t.Fatalf("conflict range %q", cr)
	}

	key, err := r.GetKey(ctx, kv.FirstGreaterThan(k(prefix, "k05")))
	if err != nil || string(key) != string(k(prefix, "k06")) {
		t.Fatalf("getKey: %q %v", key, err)
	}
	key, err = r.GetKey(ctx, kv.LastLessThan(k(prefix, "k05")).Add(-2))
	if err != nil || string(key) != string(k(prefix, "k02")) {
		t.Fatalf("getKey backward: %q %v", key, err)
	}

	addrs, err := r.AddressesForKey(ctx, k(prefix, "k01"), true)
	if err != nil || len(addrs) == 0 {
		t.Fatalf("addresses: %v %v", addrs, err)
	}
	size, err := r.EstimatedRangeSize(ctx, prefix, end)
	if err != nil || size < 0 {
		t.Fatalf("estimated size: %d %v", size, err)
	}
	pts, err := r.RangeSplitPoints(ctx, prefix, end, 1000)
	if err != nil || len(pts) < 2 || !bytes.Equal(pts[0], prefix) || !bytes.Equal(pts[len(pts)-1], end) {
		t.Fatalf("split points: %q %v", pts, err)
	}
	if len(db.KnownStorageServers()) == 0 {
		t.Fatal("no known storage servers")
	}
}

func TestIntegrationCommitConflict(t *testing.T) {
	db, prefix := openTestDB(t)
	ctx := testCtx(t)
	x := k(prefix, "x")
	if err := commitPlan(ctx, db, setPlan(x, []byte("0"))); err != nil {
		t.Fatal(err)
	}
	a := NewNativeTxn(db, DefaultNativeOptions())
	defer a.Close()
	if _, err := a.Reader().Get(ctx, x); err != nil {
		t.Fatal(err)
	}
	if err := commitPlan(ctx, db, setPlan(x, []byte("1"))); err != nil {
		t.Fatal(err)
	}
	plan := setPlan(k(prefix, "y"), []byte("a"))
	plan.ReadConflicts = []kv.KeyRange{pointRange(x)}
	err := a.Commit(plan)
	if err != fdberr.NotCommitted {
		t.Fatalf("commit after conflicting write: %v", err)
	}
	if o := a.Shared.OutcomeNow(); o.State != OutcomeFailed {
		t.Fatalf("outcome %v", o)
	}
	delay, err := a.OnError(err)
	if err != nil || delay < 0 || delay > time.Second {
		t.Fatalf("on_error: %v %v", delay, err)
	}
	// The retry sees the new value and commits.
	v, err := a.Reader().Get(ctx, x)
	if err != nil || string(v) != "1" {
		t.Fatalf("retry read %q %v", v, err)
	}
	if err := a.Commit(plan); err != nil {
		t.Fatal(err)
	}
	o := a.Shared.OutcomeNow()
	cv := a.Shared.CommittedVersion.Load()
	if o.State != OutcomeCommitted || !o.HasVersionstamp || cv <= 0 {
		t.Fatalf("outcome %v version %d", o, cv)
	}
	if got := int64(binary.BigEndian.Uint64(o.Versionstamp[:8])); got != cv {
		t.Fatalf("versionstamp %x does not start with commit version %x", o.Versionstamp, cv)
	}

	// Conflicting key reporting.
	b := NewNativeTxn(db, DefaultNativeOptions())
	defer b.Close()
	b.Opts.ReportConflictingKeys = true
	b.OptionsChanged()
	if _, err := b.Reader().Get(ctx, x); err != nil {
		t.Fatal(err)
	}
	if err := commitPlan(ctx, db, setPlan(x, []byte("2"))); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(plan); err != fdberr.NotCommitted {
		t.Fatalf("commit: %v", err)
	}
	ranges, ok := b.Shared.Conflicting()
	if !ok || len(ranges) != 1 || !bytes.Equal(ranges[0].Begin, x) {
		t.Fatalf("conflicting ranges %q %v", ranges, ok)
	}
}

func TestIntegrationReadOnlyAndOptions(t *testing.T) {
	db, prefix := openTestDB(t)
	ctx := testCtx(t)
	tr := NewNativeTxn(db, DefaultNativeOptions())
	defer tr.Close()
	if err := tr.Commit(ryw.CommitPlan{}); err != nil {
		t.Fatalf("read-only commit: %v", err)
	}
	if o := tr.Shared.OutcomeNow(); o.State != OutcomeCommitted || o.HasVersionstamp {
		t.Fatalf("read-only outcome %v", o)
	}
	if v := tr.Shared.CommittedVersion.Load(); v != -1 {
		t.Fatalf("read-only committed version %d", v)
	}
	ro := NewNativeTxn(db, DefaultNativeOptions())
	defer ro.Close()
	ro.Opts.ReadOnly = true
	if err := ro.Commit(setPlan(k(prefix, "a"), []byte("b"))); err != fdberr.TransactionReadOnly {
		t.Fatalf("read_only commit: %v", err)
	}
	big := NewNativeTxn(db, DefaultNativeOptions())
	defer big.Close()
	big.Opts.SizeLimit = 32
	if err := big.Commit(setPlan(k(prefix, "a"), bytes.Repeat([]byte("v"), 100))); err != fdberr.TransactionTooLarge {
		t.Fatalf("size limit: %v", err)
	}
	fixed := NewNativeTxn(db, DefaultNativeOptions())
	defer fixed.Close()
	if err := fixed.Shared.SetReadVersion(0); err != fdberr.VersionInvalid {
		t.Fatalf("set version 0: %v", err)
	}
	rv, _ := tr.Shared.ReadVersion(ctx)
	if err := fixed.Shared.SetReadVersion(rv.Version); err != nil {
		t.Fatal(err)
	}
	if err := fixed.Shared.SetReadVersion(rv.Version); err != fdberr.ReadVersionAlreadySet {
		t.Fatalf("second set: %v", err)
	}
	if _, err := fixed.Reader().Get(ctx, k(prefix, "a")); err != nil {
		t.Fatal(err)
	}
	// Reads of an ended attempt fail with transaction_cancelled.
	fixed.Reset()
	if _, err := tr.Reader().GetRange(ctx, kv.FirstGreaterOrEqual(prefix), kv.FirstGreaterOrEqual(k(prefix, "\xff")), kv.DefaultLimits(), false); err != nil {
		t.Fatal(err)
	}
	old := tr.Reader()
	tr.Reset()
	if _, err := old.GetKey(ctx, kv.LastLessThan(k(prefix, "z")).Add(-3)); err != fdberr.TransactionCancelled {
		t.Fatalf("read on ended attempt: %v", err)
	}
	// The metadata version key is served from the read version reply.
	if _, err := tr.Reader().Get(ctx, kv.MetadataVersionKey); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationWatch(t *testing.T) {
	db, prefix := openTestDB(t)
	ctx := testCtx(t)
	w := k(prefix, "w")
	if err := commitPlan(ctx, db, setPlan(w, []byte("0"))); err != nil {
		t.Fatal(err)
	}
	tr := NewNativeTxn(db, DefaultNativeOptions())
	defer tr.Close()
	r := tr.Reader()
	v, err := r.Get(ctx, w)
	if err != nil {
		t.Fatal(err)
	}
	fired, _, err := r.RegisterWatch(w, v)
	if err != nil {
		t.Fatal(err)
	}
	// A second watch on the same key and value shares the storage watch.
	tr2 := NewNativeTxn(db, DefaultNativeOptions())
	defer tr2.Close()
	fired2, _, err := tr2.Reader().RegisterWatch(w, v)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Commit(ryw.CommitPlan{}); err != nil {
		t.Fatal(err)
	}
	if err := tr2.Commit(ryw.CommitPlan{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fired:
		t.Fatal("watch fired before the value changed")
	case <-time.After(300 * time.Millisecond):
	}
	if err := commitPlan(ctx, db, setPlan(w, []byte("1"))); err != nil {
		t.Fatal(err)
	}
	if err := waitWatchResult(ctx, fired); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if err := waitWatchResult(ctx, fired2); err != nil {
		t.Fatalf("shared watch: %v", err)
	}

	// A watch whose value is already stale fires at once.
	tr3 := NewNativeTxn(db, DefaultNativeOptions())
	defer tr3.Close()
	stale, _, err := tr3.Reader().RegisterWatch(w, []byte("0"))
	if err != nil {
		t.Fatal(err)
	}
	if err := tr3.Commit(ryw.CommitPlan{}); err != nil {
		t.Fatal(err)
	}
	if err := waitWatchResult(ctx, stale); err != nil {
		t.Fatalf("stale watch: %v", err)
	}

	// Watches of a failed commit fail with its error; of an ended attempt with transaction_cancelled.
	tr4 := NewNativeTxn(db, DefaultNativeOptions())
	defer tr4.Close()
	cancelled, _, err := tr4.Reader().RegisterWatch(w, []byte("1"))
	if err != nil {
		t.Fatal(err)
	}
	tr4.Reset()
	if err := waitWatchResult(ctx, cancelled); err != fdberr.TransactionCancelled {
		t.Fatalf("watch of reset attempt: %v", err)
	}

	// The max_watches limit.
	db.Watches.SetMax(0)
	if _, _, err := tr4.Reader().RegisterWatch(w, nil); err != fdberr.TooManyWatches {
		t.Fatalf("over limit: %v", err)
	}
	db.Watches.SetMax(defaultMaxWatches)
}

func TestIntegrationCloseStopsEverything(t *testing.T) {
	path := os.Getenv("FDB_CLUSTER_FILE")
	if path == "" {
		t.Skip("FDB_CLUSTER_FILE not set")
	}
	rec, err := cluster.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	db := NewDbContext(*rec, false)
	ctx := testCtx(t)
	tr := NewNativeTxn(db, DefaultNativeOptions())
	tr.Opts.UseGRVCache = true
	tr.OptionsChanged()
	if _, err := tr.Shared.ReadVersion(ctx); err != nil {
		t.Fatal(err)
	}
	fired, _, err := tr.Reader().RegisterWatch([]byte("native_test/close/none"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Commit(ryw.CommitPlan{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { db.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not stop the background goroutines")
	}
	if err := waitWatchResult(ctx, fired); err != fdberr.OperationCancelled {
		t.Fatalf("watch after close: %v", err)
	}
	tr2 := NewNativeTxn(db, DefaultNativeOptions())
	if _, err := tr2.Shared.ReadVersion(ctx); err != fdberr.OperationCancelled {
		t.Fatalf("read version after close: %v", err)
	}
}
