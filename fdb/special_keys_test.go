package fdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestSpecialKeyRangeOffsetLoadsStatus(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	status := []byte("\xff\xff/status/json")
	after := []byte("\xff\xff/status/json\x01")
	before := []byte("\xff\xff/status/jso")
	for _, reverse := range []bool{false, true} {
		for _, r := range []SelectorRange{
			{Begin: LastLessThan(after), End: FirstGreaterOrEqual(after)},
			{Begin: FirstGreaterOrEqual(before), End: KeySelector{Key: before, Offset: 2}},
		} {
			tr := mustTr(t, db)
			if err := tr.Options().SetSpecialKeySpaceRelaxed(); err != nil {
				t.Fatal(err)
			}
			rows, err := sliceRange(ctx, tr, r, RangeOptions{Reverse: reverse})
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || !bytes.Equal(rows[0].Key, status) || !json.Valid(rows[0].Value) {
				t.Fatalf("range %+v reverse=%v returned %v; want status/json", r, reverse, rows)
			}
		}
	}
}

// readWaitContext signals when a blocking operation starts observing its
// context, after the operation has registered its issue-time view.
type readWaitContext struct {
	context.Context
	waiting chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (c *readWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	if c.release != nil {
		select {
		case <-c.release:
		case <-c.Context.Done():
		}
	}
	return c.Context.Done()
}

func TestUnknownClusterIDEndsWithAttempt(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	db, err := OpenConnectionString("review:review@" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, stop := range []string{"reset", "cancel", "timeout"} {
		t.Run(stop, func(t *testing.T) {
			tr := mustTr(t, db)
			if stop == "timeout" {
				if err := tr.Options().SetTimeout(10); err != nil {
					t.Fatal(err)
				}
			}
			readCtx := &readWaitContext{Context: ctx, waiting: make(chan struct{})}
			result := make(chan error, 1)
			go func() {
				value, err := tr.Get(readCtx, []byte("\xff\xff/cluster_id"))
				if value != nil {
					t.Errorf("cluster ID after %s = %q; want nil", stop, value)
				}
				result <- err
			}()
			if stop != "timeout" {
				select {
				case <-readCtx.waiting:
				case <-ctx.Done():
					t.Fatal("read did not start waiting")
				}
			}
			want := ErrTransactionCancelled
			switch stop {
			case "reset":
				tr.Reset()
			case "cancel":
				tr.Cancel()
			case "timeout":
				want = ErrTransactionTimedOut
			}
			if err := <-result; !errors.Is(err, want) {
				t.Fatalf("cluster ID after %s = %v; want %v", stop, err, want)
			}
		})
	}
}
