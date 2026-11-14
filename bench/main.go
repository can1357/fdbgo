// Command fdbbench is a small closed-loop throughput benchmark for FoundationDB.
//
// Workers issue back-to-back transactions of R random point reads (fired in
// parallel) and W random blind writes over a preloaded keyspace of fixed-size
// values. One libfdb_c process has a single network thread, which caps its
// throughput, so -procs fans the workers out over several child processes and
// merges their counters and latency histograms. -client fdbgo swaps libfdb_c
// (the official cgo bindings) for the pure-Go client in fdbgo/fdb.
// scripts/bench.sh sweeps both clients and collects the -json output.
//
//	fdbbench -C fdb.cluster -mode load -keys 1000000 -value 100 -c 64
//	fdbbench -C fdb.cluster -reads 10 -writes 0 -c 512 -procs 4 -d 10s
//	fdbbench -C fdb.cluster -client fdbgo -reads 10 -c 512 -d 10s
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"os"
	"os/exec"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var prefix = []byte("bench/")

type config struct {
	cluster   string
	client    string
	mode      string
	keys      int
	value     int
	conc      int
	procs     int
	dur       time.Duration
	warmup    time.Duration
	reads     int
	writes    int
	batch     int
	jsonOut   bool
	part      int
	parts     int
	childMode bool
}

// result is what one process measured; children send it to the parent as JSON.
type result struct {
	Txs     uint64
	Reads   uint64
	Writes  uint64
	Retries uint64
	Secs    float64
	Hist    hist
}

func main() {
	var c config
	flag.StringVar(&c.cluster, "C", "", "cluster file")
	flag.StringVar(&c.client, "client", "c", "c: libfdb_c via the official bindings; fdbgo: the pure-Go client")
	flag.StringVar(&c.mode, "mode", "run", "load: write every key once; run: timed random read/write transactions")
	flag.IntVar(&c.keys, "keys", 1_000_000, "keyspace size")
	flag.IntVar(&c.value, "value", 100, "value size in bytes")
	flag.IntVar(&c.conc, "c", 64, "total concurrent transactions (split across -procs)")
	flag.IntVar(&c.procs, "procs", 1, "client processes")
	flag.DurationVar(&c.dur, "d", 10*time.Second, "measured duration (run mode)")
	flag.DurationVar(&c.warmup, "warmup", 2*time.Second, "unmeasured warmup before -d (run mode)")
	flag.IntVar(&c.reads, "reads", 10, "random reads per transaction (run mode)")
	flag.IntVar(&c.writes, "writes", 0, "random writes per transaction (run mode)")
	flag.IntVar(&c.batch, "batch", 100, "keys per transaction (load mode)")
	flag.BoolVar(&c.jsonOut, "json", false, "print the summary as JSON")
	flag.IntVar(&c.part, "part", 0, "internal: child index")
	flag.IntVar(&c.parts, "parts", 1, "internal: child count")
	flag.BoolVar(&c.childMode, "child", false, "internal: emit raw result JSON for the parent")
	cpuprofile := flag.String("cpuprofile", "", "write a CPU profile of this process (procs=1) to the file")
	memprofile := flag.String("memprofile", "", "write an allocation profile of this process (procs=1) to the file")
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		must(err)
		must(pprof.StartCPUProfile(f))
		defer pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		defer func() {
			f, err := os.Create(*memprofile)
			must(err)
			must(pprof.Lookup("allocs").WriteTo(f, 0))
			must(f.Close())
		}()
	}
	if c.mode != "load" && c.mode != "run" {
		fail("unknown -mode %q", c.mode)
	}
	if c.client != "c" && c.client != "fdbgo" {
		fail("unknown -client %q", c.client)
	}
	if c.reads+c.writes == 0 {
		fail("-reads and -writes are both 0")
	}

	var r result
	if c.procs > 1 && !c.childMode {
		r = fanOut(c)
	} else {
		r = measure(c)
	}
	if c.childMode {
		must(json.NewEncoder(os.Stdout).Encode(r))
		return
	}
	report(c, r)
}

// fanOut re-executes this binary as c.procs children, each owning an equal
// share of workers (and, in load mode, of the keyspace), and merges results.
func fanOut(c config) result {
	type child struct {
		cmd *exec.Cmd
		out bytes.Buffer
	}
	children := make([]*child, c.procs)
	for i := range children {
		conc := c.conc / c.procs
		if i < c.conc%c.procs {
			conc++
		}
		ch := &child{}
		ch.cmd = exec.Command(os.Args[0],
			"-C", c.cluster, "-client", c.client, "-mode", c.mode, "-keys", strconv.Itoa(c.keys), "-value", strconv.Itoa(c.value),
			"-c", strconv.Itoa(conc), "-d", c.dur.String(), "-warmup", c.warmup.String(),
			"-reads", strconv.Itoa(c.reads), "-writes", strconv.Itoa(c.writes), "-batch", strconv.Itoa(c.batch),
			"-part", strconv.Itoa(i), "-parts", strconv.Itoa(c.procs), "-child")
		ch.cmd.Stdout = &ch.out
		ch.cmd.Stderr = os.Stderr
		must(ch.cmd.Start())
		children[i] = ch
	}
	var total result
	for i, ch := range children {
		if err := ch.cmd.Wait(); err != nil {
			fail("child %d: %v", i, err)
		}
		var r result
		must(json.Unmarshal(ch.out.Bytes(), &r))
		total.Txs += r.Txs
		total.Reads += r.Reads
		total.Writes += r.Writes
		total.Retries += r.Retries
		total.Secs = max(total.Secs, r.Secs)
		total.Hist.merge(&r.Hist)
	}
	return total
}

// measure runs this process's share of the workload and returns its counters.
func measure(c config) result {
	db, err := openKV(c)
	must(err)

	workers := make([]*worker, c.conc)
	for i := range workers {
		w := &worker{cfg: &c, db: db, rng: rand.New(rand.NewPCG(uint64(c.part), uint64(i))), val: make([]byte, c.value)}
		for j := range w.val {
			w.val[j] = byte(w.rng.Uint32())
		}
		workers[i] = w
	}

	var wg sync.WaitGroup
	var start time.Time
	var secs float64
	if c.mode == "load" {
		// Contiguous key slices: part p of parts, then worker i of conc.
		lo, hi := split(0, c.keys, c.part, c.parts)
		start = time.Now()
		for i, w := range workers {
			wlo, whi := split(lo, hi, i, c.conc)
			wg.Go(func() { w.load(wlo, whi) })
		}
		wg.Wait()
		secs = time.Since(start).Seconds()
	} else {
		var phase atomic.Int32 // 0 warmup, 1 measuring, 2 stop
		for _, w := range workers {
			wg.Go(func() { w.run(&phase) })
		}
		time.Sleep(c.warmup)
		phase.Store(1)
		start = time.Now()
		time.Sleep(c.dur)
		phase.Store(2)
		secs = time.Since(start).Seconds()
		wg.Wait()
	}

	r := result{Secs: secs}
	for _, w := range workers {
		r.Txs += w.res.Txs
		r.Reads += w.res.Reads
		r.Writes += w.res.Writes
		r.Retries += w.res.Retries
		r.Hist.merge(&w.res.Hist)
	}
	return r
}

type worker struct {
	cfg *config
	db  kv
	rng *rand.Rand
	val []byte
	res result
}

func key(i int) []byte {
	k := make([]byte, len(prefix)+8)
	copy(k, prefix)
	binary.BigEndian.PutUint64(k[len(prefix):], uint64(i))
	return k
}

func (w *worker) load(lo, hi int) {
	for i := lo; i < hi; i += w.cfg.batch {
		end := min(i+w.cfg.batch, hi)
		wkeys := make([][]byte, 0, end-i)
		for k := i; k < end; k++ {
			wkeys = append(wkeys, key(k))
		}
		t0 := time.Now()
		attempts, err := w.db.txn(nil, wkeys, w.val)
		must(err)
		w.res.Hist.add(time.Since(t0))
		w.res.Txs++
		w.res.Writes += uint64(end - i)
		w.res.Retries += uint64(attempts - 1)
	}
}

func (w *worker) run(phase *atomic.Int32) {
	c := w.cfg
	rkeys := make([][]byte, c.reads)
	wkeys := make([][]byte, c.writes)
	for {
		p := phase.Load()
		if p == 2 {
			return
		}
		for i := range rkeys {
			rkeys[i] = key(w.rng.IntN(c.keys))
		}
		for i := range wkeys {
			wkeys[i] = key(w.rng.IntN(c.keys))
		}
		t0 := time.Now()
		attempts, err := w.db.txn(rkeys, wkeys, w.val)
		must(err)
		// Count a transaction only if it both started and finished inside the window.
		if p == 1 && phase.Load() == 1 {
			w.res.Hist.add(time.Since(t0))
			w.res.Txs++
			w.res.Reads += uint64(c.reads)
			w.res.Writes += uint64(c.writes)
			w.res.Retries += uint64(attempts - 1)
		}
	}
}

// split returns the i-th of n near-equal contiguous slices of [lo, hi).
func split(lo, hi, i, n int) (int, int) {
	size := hi - lo
	return lo + size*i/n, lo + size*(i+1)/n
}

// hist is a log-linear latency histogram in microseconds: exact below 1024µs,
// then 512 sub-buckets per power of two (≤0.2% error). Sparse so it serializes small.
type hist map[int]uint64

const histLinear = 1024

func bucket(us uint64) int {
	if us < histLinear {
		return int(us)
	}
	shift := bits.Len64(us) - 10
	return histLinear + (shift-1)*512 + int(us>>shift) - 512
}

func bucketLow(b int) uint64 {
	if b < histLinear {
		return uint64(b)
	}
	b -= histLinear
	shift := b/512 + 1
	return uint64(b%512+512) << shift
}

func (h *hist) add(d time.Duration) {
	if *h == nil {
		*h = hist{}
	}
	(*h)[bucket(uint64(d.Microseconds()))]++
}

func (h *hist) merge(o *hist) {
	if *h == nil {
		*h = hist{}
	}
	for b, n := range *o {
		(*h)[b] += n
	}
}

// quantile returns the lower bound of the bucket holding quantile q.
func (h hist) quantile(q float64) time.Duration {
	var total uint64
	maxB := 0
	for b, n := range h {
		total += n
		maxB = max(maxB, b)
	}
	rank := uint64(q * float64(total))
	var seen uint64
	for b := 0; b <= maxB; b++ {
		seen += h[b]
		if seen > rank {
			return time.Duration(bucketLow(b)) * time.Microsecond
		}
	}
	return 0
}

type summary struct {
	Client    string  `json:"client"`
	Mode      string  `json:"mode"`
	Procs     int     `json:"procs"`
	Conc      int     `json:"c"`
	Reads     int     `json:"reads_per_tx"`
	Writes    int     `json:"writes_per_tx"`
	TxPerSec  float64 `json:"tx_s"`
	ReadsSec  float64 `json:"reads_s"`
	WritesSec float64 `json:"writes_s"`
	MBSec     float64 `json:"write_mb_s"`
	P50ms     float64 `json:"p50_ms"`
	P99ms     float64 `json:"p99_ms"`
	Retries   uint64  `json:"retries"`
	Secs      float64 `json:"secs"`
}

func report(c config, r result) {
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	s := summary{
		Client: c.client, Mode: c.mode, Procs: c.procs, Conc: c.conc, Reads: c.reads, Writes: c.writes,
		TxPerSec:  float64(r.Txs) / r.Secs,
		ReadsSec:  float64(r.Reads) / r.Secs,
		WritesSec: float64(r.Writes) / r.Secs,
		MBSec:     float64(r.Writes) * float64(len(prefix)+8+c.value) / r.Secs / 1e6,
		P50ms:     ms(r.Hist.quantile(0.50)),
		P99ms:     ms(r.Hist.quantile(0.99)),
		Retries:   r.Retries,
		Secs:      r.Secs,
	}
	if c.mode == "load" {
		s.Reads, s.Writes = 0, c.batch
	}
	if c.jsonOut {
		must(json.NewEncoder(os.Stdout).Encode(s))
		return
	}
	fmt.Printf("%s %s procs=%d c=%d r/tx=%d w/tx=%d  tx/s=%.0f reads/s=%.0f writes/s=%.0f (%.1f MB/s)  p50=%.2fms p99=%.2fms retries=%d in %.1fs\n",
		s.Client, s.Mode, s.Procs, s.Conc, s.Reads, s.Writes, s.TxPerSec, s.ReadsSec, s.WritesSec, s.MBSec, s.P50ms, s.P99ms, s.Retries, s.Secs)
}

func must(err error) {
	if err != nil {
		fail("%v", err)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "fdbbench: "+format+"\n", args...)
	os.Exit(1)
}
