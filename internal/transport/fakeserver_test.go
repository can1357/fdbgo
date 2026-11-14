package transport

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"

	"github.com/zeebo/xxh3"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/wire"
)

// fakeServer is a minimal FlowTransport server: it answers the handshake and
// hands every received packet to handle, on the connection's read goroutine.
type fakeServer struct {
	ln      net.Listener
	version uint64
	// connectLen is the length field of the server's ConnectPacket.
	connectLen uint32
	tlsConfig  *tls.Config
	handle     func(s *serverConn, dest wire.UID, msg []byte)

	mu    sync.Mutex
	conns []*serverConn
	wg    sync.WaitGroup
}

// serverConn is one accepted connection of a fakeServer.
type serverConn struct {
	nc       net.Conn
	checksum bool
	r        *bufio.Reader
	mu       sync.Mutex
	w        *bufio.Writer
	b        wire.Builder
	frame    []byte
	hdr      [12]byte
}

// newFakeServer listens on loopback; handle defaults to answerPings.
func newFakeServer(t testing.TB, handle func(s *serverConn, dest wire.UID, msg []byte)) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if handle == nil {
		handle = answerPings
	}
	fs := &fakeServer{ln: ln, version: ProtocolVersion | objectSerializerFlag, connectLen: connectPacketLen, handle: handle}
	t.Cleanup(fs.close)
	return fs
}

// start begins accepting; call after adjusting the server's fields.
func (fs *fakeServer) start() *fakeServer {
	fs.wg.Go(func() {
		for {
			nc, err := fs.ln.Accept()
			if err != nil {
				return
			}
			fs.wg.Go(func() { fs.serve(nc) })
		}
	})
	return fs
}

func (fs *fakeServer) addr() wire.NetAddr {
	return wire.NetAddr{Addr: netip.MustParseAddrPort(fs.ln.Addr().String()), TLS: fs.tlsConfig != nil}
}

func (fs *fakeServer) close() {
	fs.ln.Close()
	fs.mu.Lock()
	for _, c := range fs.conns {
		c.nc.Close()
	}
	fs.mu.Unlock()
	fs.wg.Wait()
}

// dropAll closes every accepted connection.
func (fs *fakeServer) dropAll() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for _, c := range fs.conns {
		c.nc.Close()
	}
}

func (fs *fakeServer) serve(nc net.Conn) {
	if fs.tlsConfig != nil {
		tc := tls.Server(nc, fs.tlsConfig)
		if err := tc.Handshake(); err != nil {
			nc.Close()
			return
		}
		nc = tc
	}
	s := &serverConn{nc: nc, checksum: fs.tlsConfig == nil, r: bufio.NewReaderSize(nc, 64<<10), w: bufio.NewWriterSize(nc, 64<<10)}
	fs.mu.Lock()
	fs.conns = append(fs.conns, s)
	fs.mu.Unlock()
	defer nc.Close()
	var hello [4 + connectPacketLen]byte
	if _, err := io.ReadFull(s.r, hello[:]); err != nil {
		return
	}
	reply := make([]byte, 4+int(max(fs.connectLen, 8)))
	binary.LittleEndian.PutUint32(reply[0:], fs.connectLen)
	binary.LittleEndian.PutUint64(reply[4:], fs.version)
	if _, err := nc.Write(reply); err != nil {
		return
	}
	hdrLen := 4
	if s.checksum {
		hdrLen = 12
	}
	var hdr [12]byte
	var buf []byte
	for {
		if _, err := io.ReadFull(s.r, hdr[:hdrLen]); err != nil {
			return
		}
		n := int(binary.LittleEndian.Uint32(hdr[:]))
		if cap(buf) < n {
			buf = make([]byte, n)
		}
		pkt := buf[:n]
		if _, err := io.ReadFull(s.r, pkt); err != nil {
			return
		}
		if s.checksum && xxh3.Hash(pkt) != binary.LittleEndian.Uint64(hdr[4:]) {
			panic("client sent a bad checksum")
		}
		dest := wire.UID{First: binary.LittleEndian.Uint64(pkt[0:]), Second: binary.LittleEndian.Uint64(pkt[8:])}
		fs.handle(s, dest, pkt[16:])
		if s.r.Buffered() == 0 {
			s.mu.Lock()
			err := s.w.Flush()
			s.mu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// send frames msg for dest; build reuses the connection's builder under its lock.
func (s *serverConn) send(dest wire.UID, build func(b *wire.Builder) []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b.Reset()
	msg := build(&s.b)
	s.frame = binary.LittleEndian.AppendUint64(s.frame[:0], dest.First)
	s.frame = binary.LittleEndian.AppendUint64(s.frame, dest.Second)
	s.frame = append(s.frame, msg...)
	hdr := s.hdr[:]
	binary.LittleEndian.PutUint32(hdr, uint32(len(s.frame)))
	if s.checksum {
		binary.LittleEndian.PutUint64(hdr[4:], xxh3.Hash(s.frame))
		s.w.Write(hdr)
	} else {
		s.w.Write(hdr[:4])
	}
	s.w.Write(s.frame)
}

// flush writes out everything sent so far.
func (s *serverConn) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Flush()
}

// answerPings replies Void to every PingRequest, whatever its destination.
func answerPings(s *serverConn, _ wire.UID, msg []byte) {
	if reply, err := wire.PingReplyToken(msg); err == nil {
		s.send(reply, wire.EncodeVoidReply)
	}
}

// errorReply writes ErrorOr<T> carrying error code.
func errorReply(code fdberr.Error) func(b *wire.Builder) []byte {
	return func(b *wire.Builder) []byte {
		b.StartTable(1)
		b.AddU16(0, uint16(code))
		e := b.EndTable()
		b.StartTable(2)
		b.AddUnion(0, 1, e)
		root := b.EndTable()
		return b.Finish(root, wire.FIDErrorOr|wire.FIDVoid)
	}
}

// noticeMsg writes an EndpointNotFound/UnauthorizedEndpoint notice naming tok.
func noticeMsg(tok wire.UID) func(b *wire.Builder) []byte {
	return func(b *wire.Builder) []byte {
		b.StartTable(1)
		b.AddUID(0, tok)
		root := b.EndTable()
		return b.Finish(root, 0)
	}
}

// paddedVoidReply writes a Void reply followed by pad unreferenced bytes.
func paddedVoidReply(pad int) func(b *wire.Builder) []byte {
	return func(b *wire.Builder) []byte {
		clear(b.Raw(pad))
		return wire.EncodeVoidReply(b)
	}
}

// pingRequest writes a PingRequest whose reply goes to reply.
func pingRequest(reply wire.UID) func(b *wire.Builder) []byte {
	return func(b *wire.Builder) []byte {
		off := wire.Ping{}.Encode(b, reply)
		return b.FinishRequest(off, wire.Ping{}.FileID())
	}
}
