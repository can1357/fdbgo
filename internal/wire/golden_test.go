package wire

import (
	"bufio"
	"encoding/hex"
	"net/netip"
	"os"
	"strings"
	"testing"

	"github.com/can1357/fdbgo/internal/kv"
)

// TestRequestGolden checks every request encoding byte-for-byte against
// recorded reference encodings of the same inputs with reply token UID{1, 2}
// (testdata/requests.golden).
func TestRequestGolden(t *testing.T) {
	f, err := os.Open("testdata/requests.golden")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	want := map[string]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		name, h, _ := strings.Cut(sc.Text(), " ")
		want[name] = h
	}

	none := ReadCtx{Version: 1234567}
	tags := [][]byte{[]byte("t1")}
	full := ReadCtx{Version: 42, Tags: tags, Options: ReadOpts{ReadType: 3, CacheResult: true, LockAware: true}, HasOptions: true}
	a := kv.KeySelector{Key: []byte("a"), OrEqual: false, Offset: 1}
	z := kv.KeySelector{Key: []byte("z"), OrEqual: true, Offset: 5}
	muts := []Mutation{
		{Kind: 0, Param1: []byte("k"), Param2: []byte("v")},
		{Kind: 2, Param1: []byte("n"), Param2: []byte("\x01\x00\x00\x00")},
		{Kind: 1, Param1: []byte("x"), Param2: []byte("y")},
	}
	rc := []kv.KeyRange{{Begin: []byte("a"), End: []byte("b")}}
	wc := []kv.KeyRange{{Begin: []byte("c"), End: []byte("d")}, {Begin: []byte("e"), End: []byte("f")}}
	idem := make([]byte, 16)
	for i := range idem {
		idem[i] = 7
	}
	m1 := StorageMetrics{Bytes: 100, BytesWrittenPerKSecond: 2, IOsPerKSecond: 3, BytesReadPerKSecond: 4, OpsReadPerKSecond: 5}

	var b Builder
	reply := UID{1, 2}
	got := map[string][]byte{}
	put := func(name string, msg []byte) { got[name] = append([]byte(nil), msg...) }
	put("get_value", EncodeRequest(&b, Request[Lb[[]byte]](&GetValue{Key: []byte("hello"), Ctx: none}), reply))
	put("get_value_full", EncodeRequest(&b, Request[Lb[[]byte]](&GetValue{Key: []byte{}, Ctx: full}), reply))
	put("get_key", EncodeRequest(&b, Request[Lb[kv.KeySelector]](&GetKey{Sel: kv.KeySelector{Key: []byte("k"), OrEqual: true, Offset: -3}, Ctx: none}), reply))
	put("get_key_values", EncodeRequest(&b, Request[Lb[RangePage]](&GetKeyValues{Begin: a, End: z, Limit: 100, LimitBytes: 80000, Ctx: full}), reply))
	put("get_mapped", EncodeRequest(&b, Request[Lb[MappedPage]](&GetMappedKeyValues{Begin: a, End: z, Mapper: []byte("m"), Limit: -1, LimitBytes: 1000, Ctx: none}), reply))
	put("watch", EncodeRequest(&b, Request[int64](&WatchValue{Key: []byte("w"), Value: []byte("v"), Version: 99}), reply))
	put("watch_absent", EncodeRequest(&b, Request[int64](&WatchValue{Key: []byte("w"), Version: 99, Tags: tags}), reply))
	put("commit", EncodeRequest(&b, Request[CommitReply](&Commit{ReadConflicts: rc, WriteConflicts: wc, Mutations: muts, ReadSnapshot: 77, ReportConflictingKeys: true}), reply))
	put("commit_full", EncodeRequest(&b, Request[CommitReply](&Commit{WriteConflicts: wc, Mutations: muts[:1], ReadSnapshot: 1, LockAware: true, Flags: 3, Tags: tags, IdempotencyID: idem}), reply))
	put("grv", EncodeRequest(&b, Request[ReadVersionReply](GetReadVersion{Count: 3}), reply))
	put("grv_tags", EncodeRequest(&b, Request[ReadVersionReply](GetReadVersion{Count: 1, Flags: 0x1000000, Tags: []TagCount{{Tag: []byte("t"), Count: 2}}}), reply))
	coords := []NetAddr{{Addr: netip.MustParseAddrPort("127.0.0.1:4500")}, {Addr: netip.MustParseAddrPort("[::1]:4501")}}
	put("open_db", EncodeRequest(&b, Request[ClientDBInfo](OpenDatabase{KnownID: UID{5, 6}, ClusterKey: []byte("test:test"), Coordinators: coords}), reply))
	put("open_db_host", EncodeRequest(&b, Request[ClientDBInfo](OpenDatabase{ClusterKey: []byte("d:i"), Hostnames: []Hostname{{Host: "example.com", Port: 4500, TLS: true}}}), reply))
	put("locations", EncodeRequest(&b, Request[[]ShardLocation](GetKeyServerLocations{Begin: []byte("a"), End: []byte{0xff}, Limit: 100}), reply))
	put("locations_rev", EncodeRequest(&b, Request[[]ShardLocation](GetKeyServerLocations{Begin: []byte("q"), Limit: 1, Reverse: true}), reply))
	put("wait_metrics", EncodeRequest(&b, Request[StorageMetrics](&WaitMetrics{Range: kv.KeyRange{Begin: []byte("a"), End: []byte("b")}, Max: m1}), reply))
	put("split", EncodeRequest(&b, Request[[][]byte](&SplitRange{Range: kv.KeyRange{Begin: []byte("a"), End: []byte("z")}, ChunkSize: 1000}), reply))
	put("ping", EncodeRequest(&b, Request[struct{}](Ping{}), reply))

	if len(got) != len(want) {
		t.Errorf("encoded %d messages, golden file has %d", len(got), len(want))
	}
	for name, w := range want {
		g, ok := got[name]
		if !ok {
			t.Errorf("%s: not encoded", name)
			continue
		}
		if h := hex.EncodeToString(g); h != w {
			t.Errorf("%s mismatch:\n got  %s\n want %s", name, h, w)
		}
	}
}
