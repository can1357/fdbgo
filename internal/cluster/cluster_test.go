package cluster

import (
	"encoding/hex"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/can1357/fdbgo/internal/fdberr"
)

func TestParsesLikeFlow(t *testing.T) {
	for in, want := range map[string]string{
		"asdf:2345@1.1.1.1:345":                               "asdf:2345@1.1.1.1:345",
		" # comment\nd_1:ab3@[::1]:4500:tls, 10.0.0.1:1 #x\n": "d_1:ab3@[::1]:4500:tls,10.0.0.1:1",
		"a:b@10.0.0.1:1(fromHostname)":                        "a:b@10.0.0.1:1",
		"a:b@1.1.1.1:1#trailing comment":                      "a:b@1.1.1.1:1",
		"a:b@127.000.000.001:4500":                            "a:b@127.0.0.1:4500",
	} {
		cs, err := ParseConnectionString(in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if cs.String() != want {
			t.Errorf("%q: got %q want %q", in, cs.String(), want)
		}
	}
	cs, err := ParseConnectionString("a:b@localhost:4500,fdb-1.example.com:4501:tls")
	if err != nil {
		t.Fatal(err)
	}
	want := []Coordinator{
		{Host: "localhost", Port: 4500},
		{Host: "fdb-1.example.com", Port: 4501, TLS: true},
	}
	if !slices.Equal(cs.Coordinators, want) {
		t.Fatalf("%+v", cs.Coordinators)
	}
	if cs.String() != "a:b@localhost:4500,fdb-1.example.com:4501:tls" {
		t.Fatal(cs.String())
	}
	if !cs.Coordinators[0].IsHostname() || !cs.Coordinators[1].IsHostname() {
		t.Fatal("hostname split")
	}
	cs, _ = ParseConnectionString("a:b@1.2.3.4:5:tls,host.:6,[::1]:7:tls")
	want = []Coordinator{
		{Addr: netip.MustParseAddrPort("1.2.3.4:5"), TLS: true},
		{Host: "host.", Port: 6},
		{Addr: netip.MustParseAddrPort("[::1]:7"), TLS: true},
	}
	if !slices.Equal(cs.Coordinators, want) {
		t.Fatalf("%+v", cs)
	}
}

func TestDefaultClusterFilePlatforms(t *testing.T) {
	for _, tt := range []struct {
		goos string
		want string
	}{
		{"darwin", "/usr/local/etc/foundationdb/fdb.cluster"},
		{"freebsd", "/usr/local/etc/foundationdb/fdb.cluster"},
		{"windows", "C:/ProgramData/foundationdb/fdb.cluster"},
		{"linux", "/etc/foundationdb/fdb.cluster"},
		{"openbsd", "/etc/foundationdb/fdb.cluster"},
	} {
		t.Run(tt.goos, func(t *testing.T) {
			if got := defaultClusterFile(tt.goos); got != tt.want {
				t.Fatalf("default cluster file = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRejectsBadStrings(t *testing.T) {
	for _, bad := range []string{
		"nokey@1.1.1.1:1", "a:b", "a-b:c@1.1.1.1:1", "a:b-c@1.1.1.1:1", "a:b@1.1.1.1:1,1.1.1.1:1", "a:b@",
		"a:b@1.1.1.1", "a:b@1.1.1.1:70000", "a:b@[::1:4500", "a:b@fe80::1%en0:4500", "a:b@.host:1",
		"a:b@foo..bar.:4500", "a:b@foo..:4500", "a:b@256.000.0.1:4500",
	} {
		if _, err := ParseConnectionString(bad); err != fdberr.ConnectionStringInvalid {
			t.Errorf("%q: got %v", bad, err)
		}
	}
}

func TestOpenPersistAndLookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fdb.cluster")
	if err := os.WriteFile(path, []byte("# my cluster\ntest:abc@127.0.0.1:4500\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	rec, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Path != path || rec.Conn.String() != "test:abc@127.0.0.1:4500" {
		t.Fatalf("%+v", rec)
	}
	next, _ := ParseConnectionString("test:abc@127.0.0.1:4500,127.0.0.2:4500:tls")
	rec.Persist(next)
	stored, ok, err := rec.Stored()
	if !ok || err != nil || stored.String() != next.String() {
		t.Fatalf("stored %v %v %v", stored, ok, err)
	}
	text, _ := os.ReadFile(path)
	if !strings.HasPrefix(string(text), "# DO NOT EDIT!\n") {
		t.Fatalf("%q", text)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "fdb.cluster" {
		t.Fatalf("unexpected files after persist: %v, %v", entries, err)
	}

	mem := ConnectionRecord{Conn: next}
	mem.Init()
	mem.Persist(next)
	if _, ok, _ := mem.Stored(); ok {
		t.Fatal("memory record reports a stored string")
	}

	if _, err := Open(filepath.Join(dir, "missing")); err != fdberr.NoClusterFileFound {
		t.Fatalf("got %v", err)
	}
	big := filepath.Join(dir, "big.cluster")
	os.WriteFile(big, []byte("a:b@1.1.1.1:1\n"+strings.Repeat("#", maxClusterFileBytes)), 0o644)
	if _, err := Open(big); err != fdberr.ConnectionStringInvalid {
		t.Fatalf("got %v", err)
	}
	bad := filepath.Join(dir, "bad.cluster")
	os.WriteFile(bad, []byte("garbage"), 0o644)
	if _, err := Open(bad); err != fdberr.ConnectionStringInvalid {
		t.Fatalf("got %v", err)
	}

	if Lookup("x") != "x" {
		t.Fatal("explicit path")
	}
	t.Setenv(clusterFileEnv, path)
	if Lookup("") != path {
		t.Fatal("env path")
	}
	rec, err = Open("")
	if err != nil || rec.Path != path {
		t.Fatal(rec, err)
	}
	os.Unsetenv(clusterFileEnv)
	t.Chdir(dir)
	if Lookup("") != "fdb.cluster" {
		t.Fatal("./fdb.cluster")
	}
	t.Chdir(t.TempDir())
	if Lookup("") != DefaultClusterFile() {
		t.Fatal("platform default")
	}
}

func TestConnectionRecordMemoryLocation(t *testing.T) {
	conn, err := ParseConnectionString("memory:original@127.0.0.1:4500")
	if err != nil {
		t.Fatal(err)
	}
	rec := ConnectionRecord{Conn: conn}
	rec.Init()
	location := rec.Location()
	if id, err := hex.DecodeString(location); err != nil || len(id) != 16 || strings.ToLower(location) != location {
		t.Fatalf("memory location is not a 128-bit UID: %q", location)
	}
	rec.Init()
	if rec.Location() != location {
		t.Fatal("initializing an initialized record changed its location")
	}
	copied := rec
	next, err := ParseConnectionString("memory:forwarded@127.0.0.2:4501")
	if err != nil {
		t.Fatal(err)
	}
	rec.Persist(next)
	copied.Persist(next)
	if rec.Location() != location || copied.Location() != location {
		t.Fatal("forwarding or copying changed the memory record's identity")
	}
	independent := ConnectionRecord{Conn: conn}
	independent.Init()
	if independent.Location() == location {
		t.Fatal("independent memory records have the same identity")
	}
	file := ConnectionRecord{Path: "relative/fdb.cluster", Conn: conn}
	file.Init()
	if file.Location() != file.Path {
		t.Fatalf("file location changed: %q", file.Location())
	}
}

func TestPersistDoesNotReuseSharedTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fdb.cluster")
	shared := path + ".tmp"
	const sentinel = "another writer's unfinished update"
	if err := os.WriteFile(shared, []byte(sentinel), 0o600); err != nil {
		t.Fatal(err)
	}
	conn, err := ParseConnectionString("safe:replacement@127.0.0.1:4500")
	if err != nil {
		t.Fatal(err)
	}
	rec := ConnectionRecord{Path: path}
	rec.Persist(conn)
	stored, ok, err := rec.Stored()
	if err != nil || !ok || stored.String() != conn.String() {
		t.Fatalf("stored = %v, %v, %v", stored, ok, err)
	}
	if text, err := os.ReadFile(shared); err != nil || string(text) != sentinel {
		t.Fatalf("another writer's temporary file changed: %q, %v", text, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 2 {
		t.Fatalf("temporary files leaked: %v, %v", entries, err)
	}
}

func TestPersistConcurrentIndependentRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fdb.cluster")
	const writers = 8
	const rounds = 24
	conns := make([]ConnectionString, writers)
	contents := make(map[string]bool, writers)
	for i := range conns {
		conn, err := ParseConnectionString(fmt.Sprintf("writer%d:%s@127.0.0.1:%d",
			i, strings.Repeat(string(rune('a'+i)), 257*(i+1)), 4500+i))
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = conn
		contents["# DO NOT EDIT!\n# This file is auto-generated, it is not to be edited by hand\n"+conn.String()+"\n"] = true
	}
	rec := ConnectionRecord{Path: path}
	rec.Persist(conns[0])
	check := func() error {
		text, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !contents[string(text)] {
			return fmt.Errorf("published incomplete or mixed connection record: %q", text)
		}
		return nil
	}

	start := make(chan struct{})
	stop := make(chan struct{})
	readerDone := make(chan error, 1)
	go func() {
		<-start
		for {
			if err := check(); err != nil {
				readerDone <- err
				return
			}
			select {
			case <-stop:
				readerDone <- nil
				return
			default:
			}
		}
	}()
	var wg sync.WaitGroup
	for _, conn := range conns {
		wg.Go(func() {
			independent := ConnectionRecord{Path: path}
			<-start
			for range rounds {
				independent.Persist(conn)
			}
		})
	}
	close(start)
	wg.Wait()
	close(stop)
	if err := <-readerDone; err != nil {
		t.Fatal(err)
	}
	if err := check(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "fdb.cluster" {
		t.Fatalf("temporary files leaked: %v, %v", entries, err)
	}
}

func TestPersistPreservesFileMode(t *testing.T) {
	conn, err := ParseConnectionString("mode:replacement@127.0.0.1:4500")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []os.FileMode{0o600, 0o640, 0o664} {
		t.Run(fmt.Sprintf("%o", mode), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "fdb.cluster")
			if err := os.WriteFile(path, nil, mode); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, mode); err != nil {
				t.Fatal(err)
			}
			if runtime.GOOS != "windows" && os.Geteuid() == 0 {
				if err := os.Chown(path, 65534, 65534); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			rec := ConnectionRecord{Path: path}
			rec.Persist(conn)
			after, err := os.Stat(path)
			if err != nil || after.Mode() != before.Mode() {
				t.Fatalf("mode after replacement = %v, %v; want %v", after, err, before.Mode())
			}
			if runtime.GOOS != "windows" {
				oldStat := reflect.ValueOf(before.Sys()).Elem()
				newStat := reflect.ValueOf(after.Sys()).Elem()
				for _, field := range []string{"Uid", "Gid"} {
					if oldStat.FieldByName(field).Uint() != newStat.FieldByName(field).Uint() {
						t.Fatalf("%s changed during replacement", field)
					}
				}
			}
		})
	}
	t.Run("new-file", func(t *testing.T) {
		dir := t.TempDir()
		control := filepath.Join(dir, "control")
		if err := os.WriteFile(control, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		want, err := os.Stat(control)
		if err != nil {
			t.Fatal(err)
		}
		rec := ConnectionRecord{Path: filepath.Join(dir, "fdb.cluster")}
		rec.Persist(conn)
		got, err := os.Stat(rec.Path)
		if err != nil || got.Mode() != want.Mode() {
			t.Fatalf("new file mode = %v, %v; want %v", got, err, want.Mode())
		}
	})
}

func TestPersistFailureKeepsMemoryAndRemovesTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fdb.cluster")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	conn, err := ParseConnectionString("failure:replacement@127.0.0.1:4500")
	if err != nil {
		t.Fatal(err)
	}
	rec := ConnectionRecord{Path: path}
	rec.Persist(conn) // Renaming a regular file over a directory must fail.
	if rec.Conn.String() != conn.String() {
		t.Fatal("failed persistence did not update the in-memory connection string")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "fdb.cluster" || !entries[0].IsDir() {
		t.Fatalf("failed replacement changed the destination or leaked a temporary file: %v, %v", entries, err)
	}
}
