package transport

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"hash/crc32"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

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
	// closeMismatch models 5.0, which closes before replying to unknown versions.
	closeMismatch bool
	mismatchBytes chan int
	tlsConfig     *tls.Config
	handle        func(s *serverConn, dest wire.UID, msg []byte)

	mu           sync.Mutex
	conns        []*serverConn
	hellos       []uint64
	helloLengths []uint32
	wg           sync.WaitGroup
}

// serverConn is one accepted connection of a fakeServer.
type serverConn struct {
	nc            net.Conn
	protocol      wire.ProtocolVersion
	object        bool
	checksum      bool
	checksumBytes int
	r             *bufio.Reader
	mu            sync.Mutex
	w             *bufio.Writer
	b             wire.Builder
	frame         []byte
	hdr           [12]byte
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
	fs := &fakeServer{ln: ln, version: uint64(wire.Protocol74) | objectSerializerFlag, connectLen: connectPacketLen, handle: handle}
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
	var hello [4 + 1024]byte
	if _, err := io.ReadFull(s.r, hello[:4]); err != nil {
		return
	}
	helloLength := binary.LittleEndian.Uint32(hello[:4])
	if helloLength < 8 || helloLength > 1024 {
		return
	}
	if _, err := io.ReadFull(s.r, hello[4:4+helloLength]); err != nil {
		return
	}
	advertised := binary.LittleEndian.Uint64(hello[4:])
	fs.mu.Lock()
	version := fs.version
	fs.hellos = append(fs.hellos, advertised)
	fs.helloLengths = append(fs.helloLengths, helloLength)
	fs.mu.Unlock()
	s.protocol = wire.ProtocolVersion(version & versionMask)
	s.object = version&objectSerializerFlag != 0
	s.checksum = fs.tlsConfig == nil || s.protocol.Normalized() < wire.ProtocolVersion(0x0FDB00A551040000)
	if s.checksum {
		s.checksumBytes = 8
		if s.protocol.Normalized() < wire.ProtocolVersion(0x0FDB00B070010000) {
			s.checksumBytes = 4
		}
	}
	if fs.closeMismatch && !wire.ProtocolVersion(advertised).Compatible(s.protocol) {
		return
	}
	reply := make([]byte, 4+int(max(fs.connectLen, 8)))
	binary.LittleEndian.PutUint32(reply[0:], fs.connectLen)
	binary.LittleEndian.PutUint64(reply[4:], version)
	if _, err := nc.Write(reply); err != nil {
		return
	}
	// A real FlowTransport peer never accepts ordinary RPCs after an
	// incompatible initial advertisement, even if the client changes codecs.
	if (advertised^version)&objectSerializerFlag != 0 || !wire.ProtocolVersion(advertised).Compatible(wire.ProtocolVersion(version)) {
		if fs.mismatchBytes != nil {
			_ = nc.SetReadDeadline(time.Now().Add(5 * time.Second))
			var b [1]byte
			n, _ := s.r.Read(b[:])
			fs.mismatchBytes <- n
		}
		return
	}
	hdrLen := 4 + s.checksumBytes
	var hdr [12]byte
	var buf []byte
	for {
		if _, err := io.ReadFull(s.r, hdr[:hdrLen]); err != nil {
			return
		}
		n := int(binary.LittleEndian.Uint32(hdr[:]))
		if n < 16 || n > packetLimit {
			return
		}
		if cap(buf) < n {
			buf = make([]byte, n)
		}
		pkt := buf[:n]
		if _, err := io.ReadFull(s.r, pkt); err != nil {
			return
		}
		if s.checksumBytes == 8 && xxh3.Hash(pkt) != binary.LittleEndian.Uint64(hdr[4:]) {
			panic("client sent a bad XXH3 checksum")
		}
		if s.checksumBytes == 4 && crc32.Checksum(pkt, crc32cTable) != binary.LittleEndian.Uint32(hdr[4:]) {
			panic("client sent a bad CRC32C checksum")
		}
		dest := wire.UID{First: binary.LittleEndian.Uint64(pkt[0:]), Second: binary.LittleEndian.Uint64(pkt[8:])}
		// From 7.0, ordinary receivers use RequirePeer::Exactly, which includes
		// patch bits. Ping uses AtLeast and can keep a mismatched socket alive.
		if s.protocol.Normalized() >= wire.Protocol70.Normalized() &&
			advertised&versionMask != version&versionMask && dest != wire.WellKnown(wire.TokenPing) {
			continue
		}
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
	s.b.Protocol = s.protocol
	msg := build(&s.b)
	s.frame = binary.LittleEndian.AppendUint64(s.frame[:0], dest.First)
	s.frame = binary.LittleEndian.AppendUint64(s.frame, dest.Second)
	s.frame = append(s.frame, msg...)
	hdr := s.hdr[:]
	binary.LittleEndian.PutUint32(hdr, uint32(len(s.frame)))
	if s.checksumBytes == 8 {
		binary.LittleEndian.PutUint64(hdr[4:], xxh3.Hash(s.frame))
		s.w.Write(hdr)
	} else if s.checksumBytes == 4 {
		binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(s.frame, crc32cTable))
		s.w.Write(hdr[:8])
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
	if s.object {
		if reply, err := wire.PingReplyToken(msg, s.protocol); err == nil {
			s.send(reply, wire.EncodeVoidReply)
		}
	} else if reply, err := wire.PingReplyTokenLegacy(msg, s.protocol); err == nil {
		s.send(reply, func(b *wire.Builder) []byte {
			return wire.EncodeVoidReplyLegacy(b, s.protocol)
		})
	}
}

// errorReply writes ErrorOr<Void> carrying error code.
func errorReply(code fdberr.Error) func(b *wire.Builder) []byte {
	return func(b *wire.Builder) []byte {
		b.StartTable(1)
		b.AddU16(0, uint16(code))
		e := b.EndTable()
		b.StartTable(2)
		b.AddUnion(0, 1, e)
		root := b.EndTable()
		prefix := wire.FIDErrorOr
		if b.Protocol != 0 && b.Protocol.Normalized() < wire.Protocol70.Normalized() {
			// flow.h: ErrorOr composed identifier was 1 before 7.0.
			prefix = 1 << 24
		}
		return b.Finish(root, prefix|wire.FIDVoid)
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
		return b.FinishRequest(off, wire.MessageFileID(wire.Ping{}, b.Protocol))
	}
}
