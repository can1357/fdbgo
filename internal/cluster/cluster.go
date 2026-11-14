// Package cluster reads, parses and writes cluster files and connection strings.
//
// A connection string is description:id@coordinator,coordinator,... where
// each coordinator is ip:port, [ipv6]:port or hostname:port, optionally
// suffixed :tls. Cluster files may contain whitespace and # comments.
package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/can1357/fdbgo/internal/fdberr"
)

const (
	// clusterFileEnv names the cluster file .
	clusterFileEnv = "FDB_CLUSTER_FILE"
	// maxClusterFileBytes caps the size of a cluster file.
	maxClusterFileBytes = 60000
)

// DefaultClusterFile returns the platform default cluster file path.
func DefaultClusterFile() string {
	return defaultClusterFile(runtime.GOOS)
}

func defaultClusterFile(goos string) string {
	switch goos {
	case "darwin", "freebsd":
		return "/usr/local/etc/foundationdb/fdb.cluster"
	case "windows":
		return "C:/ProgramData/foundationdb/fdb.cluster"
	}
	return "/etc/foundationdb/fdb.cluster"
}

// Coordinator is one coordinator: a literal address (Host empty) or a
// hostname resolved at connect time (Host and Port set, Addr invalid).
type Coordinator struct {
	Addr netip.AddrPort
	Host string
	Port uint16
	TLS  bool
}

// IsHostname reports whether the coordinator is a hostname to resolve.
func (c Coordinator) IsHostname() bool { return c.Host != "" }

// String renders the coordinator as in a connection string.
func (c Coordinator) String() string {
	var s string
	if c.Host != "" {
		s = c.Host + ":" + strconv.Itoa(int(c.Port))
	} else {
		s = c.Addr.String()
	}
	if c.TLS {
		s += ":tls"
	}
	return s
}

// ConnectionString is a parsed connection string.
type ConnectionString struct {
	// Key is description:id, sent to coordinators as the cluster key.
	Key          string
	Coordinators []Coordinator
}

// String renders the connection string in canonical form.
func (cs ConnectionString) String() string {
	var b strings.Builder
	b.WriteString(cs.Key)
	b.WriteByte('@')
	for i, c := range cs.Coordinators {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(c.String())
	}
	return b.String()
}

// trim strips whitespace and #-to-end-of-line comments.
func trim(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '#':
			for i+1 < len(s) && s[i+1] != '\n' && s[i+1] != '\r' {
				i++
			}
			i++ // the line break itself
		case ' ', '\n', '\r', '\t':
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func isDigits(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func isAlnum(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z'
}

// isHostname reports whether s matches ^([\w\-]+\.?)+:\d+(:tls)?$
// but not the IPv4 form.
func isHostname(s string) bool {
	body := strings.TrimSuffix(s, ":tls")
	i := strings.LastIndexByte(body, ':')
	if i < 0 {
		return false
	}
	host, port := body[:i], body[i+1:]
	if host == "" || port == "" || !isDigits(port) {
		return false
	}
	labels := strings.Split(host, ".")
	for j, label := range labels {
		// A trailing dot is allowed; empty labels elsewhere are not.
		if label == "" && !(j > 0 && j == len(labels)-1) {
			return false
		}
		for k := range len(label) {
			if c := label[k]; !isAlnum(c) && c != '_' && c != '-' {
				return false
			}
		}
	}
	ipv4Like := strings.Contains(host, ".")
	for k := range len(host) {
		if c := host[k]; !(c >= '0' && c <= '9') && c != '.' {
			ipv4Like = false
		}
	}
	return !ipv4Like
}

// parsePort parses a decimal 16-bit port, allowing an optional leading '+'.
func parsePort(s string) (uint16, bool) {
	s = strings.TrimPrefix(s, "+")
	if s == "" || !isDigits(s) {
		return 0, false
	}
	p, err := strconv.ParseUint(s, 10, 16)
	return uint16(p), err == nil
}

// parseIP parses a literal IPv4 or IPv6 address without a zone.
func parseIP(s string) (netip.Addr, bool) {
	if !strings.ContainsRune(s, ':') {
		var octets [4]byte
		for i := range octets {
			part, rest, more := strings.Cut(s, ".")
			if more != (i < len(octets)-1) || part == "" || !isDigits(part) {
				return netip.Addr{}, false
			}
			n, err := strconv.ParseUint(part, 10, 8)
			if err != nil {
				return netip.Addr{}, false
			}
			octets[i], s = byte(n), rest
		}
		return netip.AddrFrom4(octets), true
	}
	ip, err := netip.ParseAddr(s)
	if err != nil || ip.Zone() != "" {
		return netip.Addr{}, false
	}
	return ip, true
}

func parseCoordinator(s string) (Coordinator, error) {
	invalid := fdberr.ConnectionStringInvalid
	if isHostname(s) {
		body, tls := strings.CutSuffix(s, ":tls")
		i := strings.LastIndexByte(body, ':')
		port, ok := parsePort(body[i+1:])
		if !ok {
			return Coordinator{}, invalid
		}
		return Coordinator{Host: body[:i], Port: port, TLS: tls}, nil
	}
	s = strings.TrimSuffix(s, "(fromHostname)")
	body, tls := strings.CutSuffix(s, ":tls")
	var ipStr, portStr string
	if rest, ok := strings.CutPrefix(body, "["); ok {
		ipStr, portStr, ok = strings.Cut(rest, "]:")
		if !ok {
			return Coordinator{}, invalid
		}
	} else {
		i := strings.LastIndexByte(body, ':')
		if i < 0 {
			return Coordinator{}, invalid
		}
		ipStr, portStr = body[:i], body[i+1:]
	}
	ip, ok := parseIP(ipStr)
	if !ok {
		return Coordinator{}, invalid
	}
	port, ok := parsePort(portStr)
	if !ok {
		return Coordinator{}, invalid
	}
	return Coordinator{Addr: netip.AddrPortFrom(ip, port), TLS: tls}, nil
}

// ParseConnectionString parses a connection string, rejecting malformed keys
// and duplicate coordinators with connection_string_invalid.
func ParseConnectionString(s string) (ConnectionString, error) {
	invalid := fdberr.ConnectionStringInvalid
	s = trim(s)
	key, addrs, ok := strings.Cut(s, "@")
	if !ok {
		return ConnectionString{}, invalid
	}
	desc, id, ok := strings.Cut(key, ":")
	if !ok {
		return ConnectionString{}, invalid
	}
	for i := range len(desc) {
		if c := desc[i]; !isAlnum(c) && c != '_' {
			return ConnectionString{}, invalid
		}
	}
	for i := range len(id) {
		if !isAlnum(id[i]) {
			return ConnectionString{}, invalid
		}
	}
	var coords []Coordinator
	for a := range strings.SplitSeq(addrs, ",") {
		c, err := parseCoordinator(a)
		if err != nil {
			return ConnectionString{}, err
		}
		for _, o := range coords {
			if o == c {
				return ConnectionString{}, invalid
			}
		}
		coords = append(coords, c)
	}
	return ConnectionString{Key: key, Coordinators: coords}, nil
}

// ConnectionRecord is where a database's connection string lives: the file
// it is persisted to (Path), or memory only (Path empty). Call Init before
// using a memory-only record. It is not safe for concurrent use.
type ConnectionRecord struct {
	Path string
	Conn ConnectionString

	memoryID string
}

// Init assigns a unique location to a memory-only record if it does not
// already have one. Copies of an initialized record retain its location.
func (r *ConnectionRecord) Init() {
	if r.Path == "" && r.memoryID == "" {
		r.memoryID = uniqueID()
	}
}

// Location returns the file path or, for an initialized memory-only record,
// its stable 128-bit identifier rendered as 32 lowercase hexadecimal digits.
func (r ConnectionRecord) Location() string {
	if r.Path != "" {
		return r.Path
	}
	return r.memoryID
}

func uniqueID() string {
	var id [16]byte
	_, _ = rand.Read(id[:])
	return hex.EncodeToString(id[:])
}

// Lookup resolves the cluster file name: an explicit path, else
// $FDB_CLUSTER_FILE, else ./fdb.cluster, else the platform default.
func Lookup(path string) string {
	if path != "" {
		return path
	}
	if p, ok := os.LookupEnv(clusterFileEnv); ok {
		return p
	}
	if _, err := os.Stat("fdb.cluster"); err == nil {
		return "fdb.cluster"
	}
	return DefaultClusterFile()
}

// Open loads the cluster file at path ("" for the default one). Errors are
// no_cluster_file_found, connection_string_invalid (also for files over
// 60000 bytes) and platform_error.
func Open(path string) (*ConnectionRecord, error) {
	path = Lookup(path)
	st, err := os.Stat(path)
	if err != nil {
		return nil, fdberr.NoClusterFileFound
	}
	if st.Size() > maxClusterFileBytes {
		return nil, fdberr.ConnectionStringInvalid
	}
	text, err := os.ReadFile(path)
	if err != nil {
		return nil, fdberr.PlatformError
	}
	cs, err := ParseConnectionString(string(text))
	if err != nil {
		return nil, err
	}
	return &ConnectionRecord{Path: path, Conn: cs}, nil
}

// Stored re-reads the stored connection string; ok is false when the record
// is not file-backed.
func (r *ConnectionRecord) Stored() (cs ConnectionString, ok bool, err error) {
	if r.Path == "" {
		return ConnectionString{}, false, nil
	}
	text, err := os.ReadFile(r.Path)
	if err != nil {
		return ConnectionString{}, true, fdberr.PlatformError
	}
	cs, err = ParseConnectionString(string(text))
	return cs, true, err
}

// Persist replaces the connection string and, for a file-backed record,
// writes it to the file atomically like setAndPersistConnectionString.
// A failed write is logged; the in-memory record still changes.
func (r *ConnectionRecord) Persist(cs ConnectionString) {
	r.Init()
	r.Conn = cs
	if r.Path == "" {
		return
	}
	contents := "# DO NOT EDIT!\n# This file is auto-generated, it is not to be edited by hand\n" + cs.String() + "\n"
	if err := replaceFile(r.Path, contents); err != nil {
		slog.Debug("could not persist new connection string", "path", r.Path, "error", err)
	}
}

// replaceFile publishes only complete files, even when independent records
// concurrently update the same path.
func replaceFile(path, contents string) error {
	st, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	var f *os.File
	var tmp string
	for {
		tmp = filepath.Join(filepath.Dir(path), uniqueID()+".tmp")
		// Exclusive creation prevents sharing a temporary file with another writer.
		// The mode for a new cluster file remains subject to the process umask.
		f, err = os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if !os.IsExist(err) {
			break
		}
	}
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if st != nil {
		if runtime.GOOS != "windows" {
			// Unix stat types differ by port; their Uid/Gid fields are shared.
			// Chown precedes Chmod because it can clear setuid/setgid bits.
			stat := reflect.ValueOf(st.Sys()).Elem()
			err = f.Chown(int(stat.FieldByName("Uid").Uint()), int(stat.FieldByName("Gid").Uint()))
		}
		if err == nil {
			err = f.Chmod(st.Mode())
		}
	}
	if err == nil {
		_, err = f.WriteString(contents)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp, path)
}
