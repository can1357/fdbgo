package fdb

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/wire"
)

// openTestDB opens the cluster named by FDB_CLUSTER_FILE, skipping the test
// when it is unset; the database is closed when the test ends.
func openTestDB(t testing.TB) *Database {
	t.Helper()
	path := os.Getenv("FDB_CLUSTER_FILE")
	if path == "" {
		t.Skip("FDB_CLUSTER_FILE not set")
	}
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// serverBefore checks the negotiated protocol after the test has contacted its cluster.
func serverBefore(t testing.TB, db *Database, minimum wire.ProtocolVersion) bool {
	t.Helper()
	status, err := db.ClientStatus(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Connections []struct{ ProtocolVersion string }
	}
	if err := json.Unmarshal(status, &document); err != nil {
		t.Fatal(err)
	}
	for _, connection := range document.Connections {
		if connection.ProtocolVersion == "" {
			continue
		}
		version, err := strconv.ParseUint(connection.ProtocolVersion, 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		return wire.ProtocolVersion(version).Normalized() < minimum.Normalized()
	}
	t.Fatal("no negotiated protocol after contacting cluster")
	return false
}

// unavailableBefore verifies an explicit rejection on a server lacking the feature.
func unavailableBefore(t testing.TB, db *Database, err error, minimum wire.ProtocolVersion) bool {
	t.Helper()
	if !serverBefore(t, db, minimum) {
		return false
	}
	if !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("legacy feature result = %v; want unsupported_operation", err)
	}
	return true
}

// testCtx returns a context bounding the whole test.
func testCtx(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// testPrefix returns a key prefix unique to this test run; everything under
// it is cleared when the test ends.
func testPrefix(t testing.TB, db *Database) []byte {
	t.Helper()
	p := []byte("fdbgo-test/" + t.Name() + "/" + randStr(8) + "/")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		r, _ := PrefixRange(p)
		if err := db.Transact(ctx, func(tr *Transaction) error {
			tr.ClearRange(r)
			return nil
		}); err != nil {
			t.Errorf("clearing %q: %v", p, err)
		}
	})
	return p
}

// key returns prefix + s.
func key(prefix []byte, s string) []byte { return append(slices.Clone(prefix), s...) }

// prefixRange returns the range of keys under prefix.
func prefixRange(t testing.TB, prefix []byte) KeyRange {
	t.Helper()
	r, err := PrefixRange(prefix)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

const alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// randStr returns a random alphanumeric string: watches only fire when a
// value changes, so tests write random values.
func randStr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = alphanumeric[rand.IntN(len(alphanumeric))]
	}
	return string(b)
}

// mustCommit commits tr or fails the test.
func mustCommit(t testing.TB, ctx context.Context, tr *Transaction) {
	t.Helper()
	if err := tr.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// mustTr creates a transaction or fails the test.
func mustTr(t testing.TB, db *Database) *Transaction {
	t.Helper()
	tr, err := db.CreateTransaction()
	if err != nil {
		t.Fatal(err)
	}
	return tr
}
