package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/wire"
)

func checkAdvertisements(t *testing.T, fs *fakeServer, want ...wire.ProtocolVersion) {
	t.Helper()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.hellos) != len(want) {
		t.Fatalf("handshake advertisements: got %x, want %x", fs.hellos, want)
	}
	for i, version := range want {
		want := uint64(version)
		if version.UsesObjectSerializer() {
			want |= objectSerializerFlag
		}
		if got := fs.hellos[i]; got != want {
			t.Fatalf("handshake %d advertised %#x, want %#x", i, got, want)
		}
		if got := fs.helloLengths[i]; got != uint32(connectPacketSize(version)) {
			t.Fatalf("handshake %d length %d, want %d", i, got, connectPacketSize(version))
		}
	}
}

func TestConnectNegotiatesProtocol(t *testing.T) {
	for _, version := range []wire.ProtocolVersion{
		wire.Protocol50, wire.Protocol51, wire.Protocol52, wire.Protocol60,
		wire.Protocol61, wire.Protocol62, wire.Protocol63, wire.Protocol70,
		wire.Protocol71, wire.Protocol72, wire.Protocol73, wire.Protocol74,
		wire.Protocol80, wire.Protocol74 | 0x1234, wire.Protocol73 | 0x5678,
	} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			fs := newFakeServer(t, nil)
			fs.version = uint64(version)
			if version.UsesObjectSerializer() {
				fs.version |= objectSerializerFlag
			}
			fs.connectLen = uint32(connectPacketSize(version))
			fs.closeMismatch = version.Compatible(wire.Protocol50)
			fs.start()
			c := connect(t, fs)
			if got := c.ProtocolVersion(); got != version {
				t.Fatalf("negotiated protocol: got %#x, want %#x", got, version)
			}
			if got := c.UsesObjectSerializer(); got != version.UsesObjectSerializer() {
				t.Fatalf("negotiated serializer: object=%v", got)
			}
			wantChecksum := 8
			if version.Normalized() < wire.Protocol70.Normalized() {
				wantChecksum = 4
			}
			if c.checksumBytes != wantChecksum {
				t.Fatalf("checksum width: got %d, want %d", c.checksumBytes, wantChecksum)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if _, err := Call(ctx, c, echoTok, wire.Ping{}); err != nil {
				t.Fatalf("request after exact-version negotiation: %v", err)
			}
			if version == wire.Protocol74 {
				checkAdvertisements(t, fs, wire.Protocol74)
			} else {
				checkAdvertisements(t, fs, wire.Protocol74, version)
			}
		})
	}
}

// UnauthorizedEndpoint was inserted at token 2 in 7.2. Before that,
// coordinator GetLeader/OpenDatabase used 2/3, while ProtocolInfo was still 10.
func TestHistoricalCoordinatorDestinations(t *testing.T) {
	for _, version := range []wire.ProtocolVersion{
		wire.Protocol50, wire.Protocol51, wire.Protocol52, wire.Protocol60,
		wire.Protocol61, wire.Protocol62, wire.Protocol63, wire.Protocol70,
		wire.Protocol71, wire.Protocol72, wire.Protocol73, wire.Protocol74,
		wire.Protocol80,
	} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			getLeader := wire.WellKnown(3)
			openDatabase := wire.WellKnown(wire.TokenOpenDatabase)
			peerLeader, peerDatabase := getLeader, openDatabase
			if version.Normalized() < wire.Protocol72 {
				peerLeader.Second--
				peerDatabase.Second--
			}
			leaders := make(chan struct{}, 3)
			fs := newFakeServer(t, func(s *serverConn, dest wire.UID, msg []byte) {
				switch dest {
				case peerLeader:
					leaders <- struct{}{}
					answerPings(s, dest, msg)
				case peerDatabase:
					s.send(wire.WellKnown(wire.TokenEndpointNotFound), func(b *wire.Builder) []byte {
						if version.Normalized() < wire.Protocol70.Normalized() {
							return oldNotice(b, version, dest, s.object)
						}
						return noticeMsg(dest)(b)
					})
				case wire.WellKnown(10), echoTok, wire.WellKnown(wire.TokenPing):
					answerPings(s, dest, msg)
				default:
					t.Errorf("unexpected peer destination: %+v", dest)
				}
			})
			fs.version = uint64(version)
			if version.UsesObjectSerializer() {
				fs.version |= objectSerializerFlag
			}
			fs.start()
			c := connect(t, fs)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if _, err := Call(ctx, c, getLeader, wire.Ping{}); err != nil {
				t.Fatalf("GetLeader destination: %v", err)
			}
			if _, err := Call(ctx, c, openDatabase, wire.Ping{}); err != EndpointNotFound {
				t.Fatalf("OpenDatabase notice destination: %v", err)
			}
			replies := CallBatch(ctx, []BatchCall[struct{}]{
				{c, getLeader, wire.Ping{}},
				{c, openDatabase, wire.Ping{}},
				{c, wire.WellKnown(10), wire.Ping{}},
				{c, echoTok, wire.Ping{}},
			})
			for i, want := range []error{nil, EndpointNotFound, nil, nil} {
				if replies[i].Err != want {
					t.Errorf("batch destination %d: got %v, want %v", i, replies[i].Err, want)
				}
			}
			c.Send(getLeader, wire.Ping{})
			for range 3 {
				select {
				case <-leaders:
				case <-ctx.Done():
					t.Fatal("GetLeader destination was not resolved for every send path")
				}
			}
		})
	}
}

func TestUnauthorizedEndpointNoticeVersion(t *testing.T) {
	for _, version := range []wire.ProtocolVersion{wire.Protocol71, wire.Protocol72} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			c := &Connection{protocol: version, object: true}
			for i := range c.pending {
				c.pending[i].m = make(map[uint64]pending)
			}
			slot, err := registerCall(c, goneTok)
			if err != nil {
				t.Fatal(err)
			}
			var b wire.Builder
			b.Reset()
			b.Protocol = version
			noticeMsg(goneTok)(&b)
			tok := b.Raw(16)
			binary.LittleEndian.PutUint64(tok, ^uint64(0))
			binary.LittleEndian.PutUint64(tok[8:], wire.TokenUnauthorizedEndpoint)
			c.deliver(b.Finished())
			select {
			case reply := <-slot.w:
				if version < wire.Protocol72 || reply.err != Unauthorized {
					t.Fatalf("unexpected unauthorized result: %v", reply.err)
				}
			default:
				if version >= wire.Protocol72 {
					t.Fatal("unauthorized notice was not delivered")
				}
				if !c.remove(slot.id) {
					t.Fatal("old coordinator token 2 removed a pending request")
				}
			}
		})
	}
}

func TestConnectionsKeepIndependentProtocols(t *testing.T) {
	versions := []wire.ProtocolVersion{wire.Protocol50, wire.Protocol61, wire.Protocol62, wire.Protocol63, wire.Protocol70, wire.Protocol73, wire.Protocol74, wire.Protocol80}
	calls := make([]BatchCall[struct{}], 0, len(versions))
	for _, version := range versions {
		fs := newFakeServer(t, nil)
		fs.version = uint64(version)
		if version.UsesObjectSerializer() {
			fs.version |= objectSerializerFlag
		}
		fs.connectLen = uint32(connectPacketSize(version))
		fs.start()
		calls = append(calls, BatchCall[struct{}]{
			Connection:  connect(t, fs),
			Destination: echoTok,
			Request:     wire.Ping{},
		})
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for i, reply := range CallBatch(ctx, calls) {
		if reply.Err != nil {
			t.Fatalf("mixed-version batch reply %d: %v", i, reply.Err)
		}
		if got := calls[i].Connection.ProtocolVersion(); got != versions[i] {
			t.Fatalf("connection %d protocol: got %#x, want %#x", i, got, versions[i])
		}
	}
}

func TestConnectRejectsUnsupportedProtocol(t *testing.T) {
	for _, version := range []uint64{
		0x0FDB00B072010000 | objectSerializerFlag,
		0x0FDB00B075000000 | objectSerializerFlag,
		uint64(wire.Protocol73) | 0x10000 | objectSerializerFlag,
		uint64(wire.Protocol74) | 0x10000 | objectSerializerFlag,
		uint64(wire.Protocol73),
		uint64(wire.Protocol74),
		uint64(wire.Protocol50) | objectSerializerFlag,
		uint64(wire.Protocol61) | objectSerializerFlag,
		uint64(wire.Protocol74) | 0x2000000000000000 | objectSerializerFlag,
	} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			fs := newFakeServer(t, testHandler)
			fs.version = version
			fs.start()
			if c, err := Connect(t.Context(), fs.addr()); err != Incompatible {
				if c != nil {
					c.Close()
				}
				t.Fatalf("unsupported protocol: got %v", err)
			}
			checkAdvertisements(t, fs, wire.Protocol74)
		})
	}
	for _, length := range []uint32{4, 8, 21, 1025} {
		t.Run(fmt.Sprintf("malformed-length-%d", length), func(t *testing.T) {
			fs := newFakeServer(t, testHandler)
			fs.version = 0x0FDB00B075000000 | objectSerializerFlag
			fs.connectLen = length
			fs.start()
			if c, err := Connect(t.Context(), fs.addr()); err != Malformed {
				if c != nil {
					c.Close()
				}
				t.Fatalf("malformed length must precede compatibility: got %v", err)
			}
			checkAdvertisements(t, fs, wire.Protocol74)
		})
	}
}

func TestReconnectNegotiatesChangedProtocol(t *testing.T) {
	for _, versions := range [][2]wire.ProtocolVersion{
		{wire.Protocol73, wire.Protocol74}, {wire.Protocol74, wire.Protocol73},
		{wire.Protocol50, wire.Protocol80}, {wire.Protocol80, wire.Protocol61},
		{wire.Protocol63, wire.Protocol70},
	} {
		t.Run(fmt.Sprintf("%x-to-%x", versions[0], versions[1]), func(t *testing.T) {
			received := make(chan struct{}, 2)
			fs := newFakeServer(t, func(s *serverConn, dest wire.UID, msg []byte) {
				if dest == silentTok {
					received <- struct{}{}
				}
				if dest != silentTok {
					answerPings(s, dest, msg)
				}
			})
			fs.version = uint64(versions[0])
			if versions[0].UsesObjectSerializer() {
				fs.version |= objectSerializerFlag
			}
			fs.start()
			old := connect(t, fs)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := Call(ctx, old, silentTok, wire.Ping{})
				result <- err
			}()
			select {
			case <-received:
			case <-ctx.Done():
				t.Fatal("outstanding request was not received")
			}
			fs.mu.Lock()
			fs.version = uint64(versions[1])
			if versions[1].UsesObjectSerializer() {
				fs.version |= objectSerializerFlag
			}
			fs.mu.Unlock()
			fs.dropAll()
			if err := <-result; err != MaybeDelivered {
				t.Fatalf("request outstanding during upgrade: got %v", err)
			}
			<-old.Done()
			current := connect(t, fs)
			if got := current.ProtocolVersion(); got != versions[1] {
				t.Fatalf("reconnected protocol: got %#x, want %#x", got, versions[1])
			}
			if old.ProtocolVersion() != versions[0] || !old.IsDead() {
				t.Fatal("failed connection was mutated or revived")
			}
			if err := call(old, silentTok); err != NotDelivered {
				t.Fatalf("new call on failed connection: got %v", err)
			}
			if err := call(current, echoTok); err != nil {
				t.Fatalf("new connection request: %v", err)
			}
			select {
			case <-received:
				t.Fatal("outstanding request was replayed")
			default:
			}
			var advertisements []wire.ProtocolVersion
			for _, version := range versions {
				advertisements = append(advertisements, wire.Protocol74)
				if version != wire.Protocol74 {
					advertisements = append(advertisements, version)
				}
			}
			checkAdvertisements(t, fs, advertisements...)
		})
	}
}

func TestNegotiationHonorsContext(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		name := "deadline"
		if cancelEarly {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			second := make(chan struct{})
			served := make(chan error, 1)
			go func() {
				served <- func() error {
					for attempt, want := range []wire.ProtocolVersion{wire.Protocol74, wire.Protocol73} {
						nc, err := ln.Accept()
						if err != nil {
							return err
						}
						defer nc.Close()
						_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
						var hello [4 + connectPacketLen]byte
						if _, err := io.ReadFull(nc, hello[:]); err != nil {
							return err
						}
						if got := binary.LittleEndian.Uint64(hello[4:]); got != uint64(want)|objectSerializerFlag {
							return fmt.Errorf("attempt %d advertised %#x, want %#x", attempt, got, want)
						}
						if attempt == 0 {
							binary.LittleEndian.PutUint64(hello[4:], uint64(wire.Protocol73)|objectSerializerFlag)
							if _, err := nc.Write(hello[:]); err != nil {
								return err
							}
						} else {
							close(second) // Leave the matching handshake unanswered.
						}
						var extra [1]byte
						if n, err := nc.Read(extra[:]); n != 0 || err != io.EOF {
							return fmt.Errorf("attempt %d was not closed cleanly: %d bytes, %v", attempt, n, err)
						}
					}
					return nil
				}()
			}()
			ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				c, err := Connect(ctx, (&fakeServer{ln: ln}).addr())
				if c != nil {
					c.Close()
				}
				result <- err
			}()
			select {
			case <-second:
			case err := <-result:
				t.Fatalf("Connect returned before redial: %v", err)
			}
			want := context.DeadlineExceeded
			if cancelEarly {
				cancel()
				want = context.Canceled
			}
			if err := <-result; !errors.Is(err, want) {
				t.Fatalf("negotiation context: got %v, want %v", err, want)
			}
			if err := <-served; err != nil {
				t.Fatal(err)
			}
		})
	}
}
