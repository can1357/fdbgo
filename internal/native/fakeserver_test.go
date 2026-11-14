package native

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"

	"github.com/zeebo/xxh3"

	"github.com/can1357/fdbgo/internal/wire"
)

// nativeTestServer implements the plaintext Flow framing needed by native RPC tests.
// Monitor pings are answered automatically; handle receives other requests.
type nativeTestServer struct {
	t      testing.TB
	ln     net.Listener
	handle func(*nativeTestConn, wire.UID, []byte)

	mu     sync.Mutex
	closed bool
	conns  []net.Conn
	wg     sync.WaitGroup
}

type nativeTestConn struct {
	nc net.Conn
	mu sync.Mutex
	w  *bufio.Writer
	b  wire.Builder
}

func newNativeTestServer(t testing.TB, handle func(*nativeTestConn, wire.UID, []byte)) *nativeTestServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &nativeTestServer{t: t, ln: ln, handle: handle}
	t.Cleanup(s.close)
	s.wg.Go(func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				nc.Close()
				return
			}
			s.conns = append(s.conns, nc)
			s.mu.Unlock()
			s.wg.Go(func() { s.serve(nc) })
		}
	})
	return s
}

func (s *nativeTestServer) addr() wire.NetAddr {
	return wire.NetAddr{Addr: netip.MustParseAddrPort(s.ln.Addr().String())}
}

func (s *nativeTestServer) close() {
	s.ln.Close()
	s.mu.Lock()
	s.closed = true
	for _, nc := range s.conns {
		nc.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *nativeTestServer) serve(nc net.Conn) {
	defer nc.Close()
	r := bufio.NewReader(nc)
	c := &nativeTestConn{nc: nc, w: bufio.NewWriter(nc)}
	// ConnectPacket is a four-byte length followed by 40 bytes of fields.
	var hello [44]byte
	if _, err := io.ReadFull(r, hello[:]); err != nil {
		return
	}
	clear(hello[:])
	binary.LittleEndian.PutUint32(hello[:], 40)
	binary.LittleEndian.PutUint64(hello[4:], uint64(wire.Protocol74)|1<<60)
	if _, err := nc.Write(hello[:]); err != nil {
		return
	}
	for {
		var hdr [12]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return
		}
		n := binary.LittleEndian.Uint32(hdr[:])
		if n < 16 || n > 1<<20 {
			s.t.Errorf("invalid native test packet length: %d", n)
			return
		}
		pkt := make([]byte, int(n))
		if _, err := io.ReadFull(r, pkt); err != nil {
			return
		}
		if xxh3.Hash(pkt) != binary.LittleEndian.Uint64(hdr[4:]) {
			s.t.Error("native request checksum mismatch")
			return
		}
		dest := wire.UID{First: binary.LittleEndian.Uint64(pkt), Second: binary.LittleEndian.Uint64(pkt[8:])}
		if dest == wire.WellKnown(wire.TokenPing) {
			reply, err := wire.PingReplyToken(pkt[16:])
			if err != nil {
				s.t.Errorf("decode native monitor ping: %v", err)
				return
			}
			if err := c.send(reply, wire.EncodeVoidReply); err != nil {
				return
			}
			continue
		}
		s.handle(c, dest, pkt[16:])
	}
}

// send builds, frames and flushes one reply; concurrent replies are serialized.
func (c *nativeTestConn) send(dest wire.UID, build func(*wire.Builder) []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.b.Reset()
	msg := build(&c.b)
	frame := binary.LittleEndian.AppendUint64(nil, dest.First)
	frame = binary.LittleEndian.AppendUint64(frame, dest.Second)
	frame = append(frame, msg...)
	var hdr [12]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(frame)))
	binary.LittleEndian.PutUint64(hdr[4:], xxh3.Hash(frame))
	if _, err := c.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := c.w.Write(frame); err != nil {
		return err
	}
	return c.w.Flush()
}

// nativeRequestReplyToken reads the ReplyPromise at slot in a request table.
func nativeRequestReplyToken(msg []byte, slot int) (wire.UID, error) {
	root, err := wire.Root(msg)
	if err != nil {
		return wire.UID{}, err
	}
	req, ok, err := root.Table(0)
	if err != nil {
		return wire.UID{}, err
	}
	if !ok {
		return wire.UID{}, wire.ErrMalformed
	}
	rp, ok, err := req.Table(slot)
	if err != nil {
		return wire.UID{}, err
	}
	if !ok {
		return wire.UID{}, wire.ErrMalformed
	}
	return rp.UID(0)
}
