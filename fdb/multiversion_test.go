package fdb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// upgradeCluster owns a single-process persistent cluster, never a user's cluster.
type upgradeCluster struct {
	t                         *testing.T
	dir, clusterFile, address string
	cmd                       *exec.Cmd
	log                       *os.File
}

func newUpgradeCluster(t *testing.T, ctx context.Context, sdk string) *upgradeCluster {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := ln.Addr().String()
	ln.Close()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "data"), 0700); err != nil {
		t.Fatal(err)
	}
	c := &upgradeCluster{t: t, dir: dir, clusterFile: filepath.Join(dir, "fdb.cluster"), address: address}
	if err := os.WriteFile(c.clusterFile, []byte("upgrade:test@"+address), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.stop()
		if t.Failed() {
			if data, err := os.ReadFile(filepath.Join(dir, "server.out")); err == nil {
				t.Logf("server output:\n%s", data)
			}
		}
	})
	c.start(ctx, sdk)
	cli := exec.CommandContext(ctx, filepath.Join(sdk, "bin", "fdbcli"), "-C", c.clusterFile, "--timeout", "60", "--exec", "configure new single ssd")
	if out, err := cli.CombinedOutput(); err != nil {
		t.Fatalf("configure cluster: %v\n%s", err, out)
	}
	return c
}

func (c *upgradeCluster) start(ctx context.Context, sdk string) {
	c.t.Helper()
	var err error
	c.log, err = os.OpenFile(filepath.Join(c.dir, "server.out"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		c.t.Fatal(err)
	}
	c.cmd = exec.CommandContext(ctx, filepath.Join(sdk, "bin", "fdbserver"), "-p", c.address, "-C", c.clusterFile,
		"-d", filepath.Join(c.dir, "data"), "-L", c.dir)
	c.cmd.Stdout, c.cmd.Stderr = c.log, c.log
	if err := c.cmd.Start(); err != nil {
		c.log.Close()
		c.cmd = nil
		c.t.Fatal(err)
	}
}

func (c *upgradeCluster) stop() {
	if c.cmd == nil {
		return
	}
	_ = c.cmd.Process.Kill()
	_ = c.cmd.Wait()
	_ = c.log.Close()
	c.cmd = nil
}

func TestMultiVersionUpgrade(t *testing.T) {
	from, to := os.Getenv("FDB_UPGRADE_FROM"), os.Getenv("FDB_UPGRADE_TO")
	if from == "" || to == "" {
		t.Skip("FDB_UPGRADE_FROM and FDB_UPGRADE_TO SDK directories not set")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	oldCluster := newUpgradeCluster(t, ctx, from)
	newCluster := newUpgradeCluster(t, ctx, to)
	open := func(c *upgradeCluster) *Database {
		db, err := Open(c.clusterFile)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}
	db, other := open(oldCluster), open(newCluster)
	put := func(db *Database, value string) {
		t.Helper()
		if err := db.Transact(ctx, func(tr *Transaction) error { tr.Set([]byte("value"), []byte(value)); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	get := func(db *Database, want string) {
		t.Helper()
		v, err := ReadTransact(ctx, db, func(tr ReadTransaction) ([]byte, error) { return tr.Get(ctx, []byte("value")) })
		if err != nil || string(v) != want {
			t.Fatalf("value = %q, %v; want %q", v, err, want)
		}
	}
	put(db, "before")
	put(other, "independent")
	get(db, "before")
	get(other, "independent")
	watch, err := Transact(ctx, db, func(tr *Transaction) (*Watch, error) { return tr.Watch([]byte("value")), nil })
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Cancel()
	stale := mustTr(t, db)
	defer stale.Close()
	if v, err := stale.Get(ctx, []byte("value")); err != nil || string(v) != "before" {
		t.Fatalf("old snapshot = %q, %v", v, err)
	}
	stale.Set([]byte("value"), []byte("invalid"))

	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	var completed atomic.Int64
	progress := make(chan struct{}, 1)
	workerDone := make(chan error, 1)
	go func() {
		for sequence := int64(1); ; sequence++ {
			err := db.Transact(workerCtx, func(tr *Transaction) error {
				// A marker makes an uncertain commit retry idempotent while checking
				// that committed data survives the process/version replacement.
				value, err := tr.Get(workerCtx, []byte("sequence"))
				if err != nil {
					return err
				}
				previous := int64(0)
				if value != nil {
					previous, err = strconv.ParseInt(string(value), 10, 64)
					if err != nil {
						return err
					}
				}
				if previous == sequence {
					return nil
				}
				if previous != sequence-1 {
					return fmt.Errorf("sequence regressed: got %d before %d", previous, sequence)
				}
				tr.Set([]byte("sequence"), []byte(strconv.FormatInt(sequence, 10)))
				return nil
			})
			if err != nil {
				workerDone <- err
				return
			}
			completed.Store(sequence)
			select {
			case progress <- struct{}{}:
			default:
			}
			select {
			case <-workerCtx.Done():
				workerDone <- workerCtx.Err()
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	defer func() {
		stopWorker()
		if err := <-workerDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("transactions across upgrade: %v", err)
		}
	}()
	select {
	case <-progress:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	before := completed.Load()
	oldCluster.stop()
	oldCluster.start(ctx, to)
	// Both handles remain open: changing one cluster must not change the other.
	get(other, "independent")
	get(db, "before")
	put(db, "after")
	if err := watch.Wait(ctx); err != nil {
		t.Fatalf("watch across upgrade: %v", err)
	}
	if err := stale.Commit(ctx); !errors.Is(err, ErrNotCommitted) && !errors.Is(err, ErrTransactionTooOld) {
		t.Fatalf("pre-upgrade conflicting snapshot commit = %v; want not_committed or transaction_too_old", err)
	}
	get(db, "after")
	for completed.Load() <= before {
		select {
		case <-progress:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	t.Logf("same handle resumed after upgrade; sequence advanced from %d to %d", before, completed.Load())
}
