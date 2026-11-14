package transport

import (
	"context"
	"testing"

	"github.com/can1357/fdbgo/internal/wire"
)

// BenchmarkCall measures one request/reply round trip over loopback TCP to
// a fake server, one call at a time.
func BenchmarkCall(b *testing.B) {
	fs := newFakeServer(b, nil).start()
	c := connect(b, fs)
	ctx := context.Background()
	dest := wire.UID{First: 1, Second: 2}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Call(ctx, c, dest, wire.Ping{}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCallParallel measures round trips from many goroutines sharing
// one connection, exercising write coalescing and reply multiplexing.
func BenchmarkCallParallel(b *testing.B) {
	fs := newFakeServer(b, nil).start()
	c := connect(b, fs)
	ctx := context.Background()
	dest := wire.UID{First: 1, Second: 2}
	b.ReportAllocs()
	b.SetParallelism(32)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := Call(ctx, c, dest, wire.Ping{}); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// BenchmarkCallPipelined keeps up to 256 blocking calls in flight.
func BenchmarkCallPipelined(b *testing.B) {
	fs := newFakeServer(b, nil).start()
	c := connect(b, fs)
	dest := wire.UID{First: 1, Second: 2}
	slots := make(chan struct{}, 256)
	b.ReportAllocs()
	for b.Loop() {
		slots <- struct{}{}
		go func() {
			if _, err := Call(context.Background(), c, dest, wire.Ping{}); err != nil {
				b.Error(err)
			}
			<-slots
		}()
	}
	for range cap(slots) {
		slots <- struct{}{}
	}
}
