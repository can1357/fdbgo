package fdb

import (
	"context"
	"iter"
	"runtime"

	"github.com/can1357/fdbgo/internal/client"
	"github.com/can1357/fdbgo/internal/kv"
)

// RangeOptions controls a range read. The zero value reads everything in
// order with StreamingModeIterator.
type RangeOptions struct {
	// Limit is the maximum number of rows; 0 means no limit.
	Limit int
	// Mode tunes batching; see StreamingMode.
	Mode StreamingMode
	// Reverse reads from the end of the range backward.
	Reverse bool
}

// rangeCursor is the position of a range read between batches.
type rangeCursor struct {
	begin, end KeySelector
	opts       RangeOptions
	// iteration is the 1-based batch counter driving StreamingModeIterator.
	iteration int
}

// cOpts returns the C API range parameters of the cursor's next batch.
func (c *rangeCursor) cOpts() client.RangeOpts {
	return client.RangeOpts{
		Limit:     c.opts.Limit,
		Mode:      int(c.opts.Mode) - 1, // public modes are the C values plus one
		Iteration: c.iteration,
		Reverse:   c.opts.Reverse,
	}
}

// advance moves the cursor past a batch of n rows whose last row has key
// lastKey; it reports false when the range is exhausted.
func (c *rangeCursor) advance(n int, more bool, lastKey []byte) bool {
	if !more || n == 0 || n == c.opts.Limit {
		return false
	}
	if c.opts.Limit > 0 {
		c.opts.Limit -= n
	}
	if c.opts.Reverse {
		c.end = FirstGreaterOrEqual(lastKey)
	} else {
		c.begin = FirstGreaterThan(lastKey)
	}
	c.iteration++
	return true
}

// forSlice switches later batches to the mode that reads everything
// requested at once.
func (c *rangeCursor) forSlice() {
	if c.opts.Limit != 0 {
		c.opts.Mode = StreamingModeExact
	} else {
		c.opts.Mode = StreamingModeWantAll
	}
}

// RangeResult holds a completed first batch and reads later batches as they
// are consumed. Slice and All return context errors if cancelled. Stopping
// iteration starts no further reads. A RangeResult is not safe for concurrent use.
type RangeResult struct {
	t        *Transaction
	start    rangeCursor
	snapshot bool
	first    kv.RangeResult
}

func newRangeResult(ctx context.Context, t *Transaction, r Range, opts RangeOptions, snapshot bool) (*RangeResult, error) {
	defer runtime.KeepAlive(t)
	begin, end := r.FDBRangeKeySelectors()
	c := rangeCursor{begin: begin, end: end, opts: opts, iteration: 1}
	first, err := t.rangeBatch(ctx, &c, snapshot)
	if err != nil {
		return nil, err
	}
	return &RangeResult{t: t, start: c, snapshot: snapshot, first: first}, nil
}

func (t *Transaction) rangeBatch(ctx context.Context, c *rangeCursor, snapshot bool) (kv.RangeResult, error) {
	defer runtime.KeepAlive(t)
	if err := ctx.Err(); err != nil {
		return kv.RangeResult{}, err
	}
	batch, wait, err := t.t.GetRange(c.begin, c.end, c.cOpts(), snapshot)
	if wait != nil {
		batch, err = wait(ctx)
	}
	return batch, operationError(ctx, err)
}

// Slice returns every row of the range, up to the limit.
func (r *RangeResult) Slice(ctx context.Context) ([]KeyValue, error) {
	defer runtime.KeepAlive(r)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c := r.start
	c.forSlice()
	batch := r.first
	var out []KeyValue
	for {
		out = append(out, batch.Data...)
		n := len(batch.Data)
		if n == 0 || !c.advance(n, batch.More, batch.Data[n-1].Key) {
			return out, nil
		}
		var err error
		batch, err = r.t.rangeBatch(ctx, &c, r.snapshot)
		if err != nil {
			return nil, err
		}
	}
}

// All iterates the rows of the range, fetching batches as needed. On
// failure it yields the error once, with a zero KeyValue, and stops.
func (r *RangeResult) All(ctx context.Context) iter.Seq2[KeyValue, error] {
	return func(yield func(KeyValue, error) bool) {
		defer runtime.KeepAlive(r)
		c := r.start
		batch := r.first
		for {
			if err := ctx.Err(); err != nil {
				yield(KeyValue{}, err)
				return
			}
			for _, row := range batch.Data {
				if !yield(row, nil) {
					return
				}
			}
			n := len(batch.Data)
			if n == 0 || !c.advance(n, batch.More, batch.Data[n-1].Key) {
				return
			}
			var err error
			batch, err = r.t.rangeBatch(ctx, &c, r.snapshot)
			if err != nil {
				yield(KeyValue{}, err)
				return
			}
		}
	}
}

// MappedKeyValue is one row of a mapped range read with the rows of the
// secondary range its mapper produced.
type MappedKeyValue struct {
	KeyValue
	// RangeBegin and RangeEnd bound the secondary range.
	RangeBegin, RangeEnd KeySelector
	// Range holds the rows of the secondary range.
	Range []KeyValue
}

// MappedRangeResult holds a completed first mapped batch and reads subsequent
// batches only as consumed. It is not safe for concurrent use.
type MappedRangeResult struct {
	t        *Transaction
	start    rangeCursor
	mapper   []byte
	snapshot bool
	first    client.MappedRangeResult
}

func newMappedRangeResult(ctx context.Context, t *Transaction, r Range, mapper []byte, opts RangeOptions, snapshot bool) (*MappedRangeResult, error) {
	defer runtime.KeepAlive(t)
	begin, end := r.FDBRangeKeySelectors()
	c := rangeCursor{begin: begin, end: end, opts: opts, iteration: 1}
	first, err := t.mappedRangeBatch(ctx, &c, mapper, snapshot)
	if err != nil {
		return nil, err
	}
	return &MappedRangeResult{t: t, start: c, mapper: mapper, snapshot: snapshot, first: first}, nil
}

func (t *Transaction) mappedRangeBatch(ctx context.Context, c *rangeCursor, mapper []byte, snapshot bool) (client.MappedRangeResult, error) {
	defer runtime.KeepAlive(t)
	if err := ctx.Err(); err != nil {
		return client.MappedRangeResult{}, err
	}
	batch, wait, err := t.t.GetMappedRange(c.begin, c.end, mapper, c.cOpts(), snapshot)
	if wait != nil {
		batch, err = wait(ctx)
	}
	return batch, operationError(ctx, err)
}

// mappedRow converts an engine row to the public type.
func mappedRow(m *client.MappedKeyValue) MappedKeyValue {
	return MappedKeyValue{
		KeyValue:   KeyValue{Key: m.Key, Value: m.Value},
		RangeBegin: m.Begin,
		RangeEnd:   m.End,
		Range:      m.Data,
	}
}

// Slice returns every row of the mapped range, up to the limit.
func (r *MappedRangeResult) Slice(ctx context.Context) ([]MappedKeyValue, error) {
	defer runtime.KeepAlive(r)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c := r.start
	c.forSlice()
	batch := r.first
	var out []MappedKeyValue
	for {
		for i := range batch.Data {
			out = append(out, mappedRow(&batch.Data[i]))
		}
		n := len(batch.Data)
		if n == 0 || !c.advance(n, batch.More, batch.Data[n-1].Key) {
			return out, nil
		}
		var err error
		batch, err = r.t.mappedRangeBatch(ctx, &c, r.mapper, r.snapshot)
		if err != nil {
			return nil, err
		}
	}
}

// All iterates the rows of the mapped range like RangeResult.All.
func (r *MappedRangeResult) All(ctx context.Context) iter.Seq2[MappedKeyValue, error] {
	return func(yield func(MappedKeyValue, error) bool) {
		defer runtime.KeepAlive(r)
		c := r.start
		batch := r.first
		for {
			if err := ctx.Err(); err != nil {
				yield(MappedKeyValue{}, err)
				return
			}
			for i := range batch.Data {
				if !yield(mappedRow(&batch.Data[i]), nil) {
					return
				}
			}
			n := len(batch.Data)
			if n == 0 || !c.advance(n, batch.More, batch.Data[n-1].Key) {
				return
			}
			var err error
			batch, err = r.t.mappedRangeBatch(ctx, &c, r.mapper, r.snapshot)
			if err != nil {
				yield(MappedKeyValue{}, err)
				return
			}
		}
	}
}
