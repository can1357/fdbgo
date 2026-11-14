package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/wire"
)

// FlowTransport::sendPacket uses crc32c_append(0, token || message, len)
// through 6.3, XXH3_64bits from 7.0, and no checksum over TLS except 5.0.
func TestLegacyFrameCRC32C(t *testing.T) {
	for _, width := range []int{0, 4} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			c := &Connection{checksumBytes: width}
			var b wire.Builder
			b.Reset()
			payload := []byte("legacy message")
			copy(b.Raw(len(payload)), payload)
			dest := wire.UID{First: 0x1020304050607080, Second: 0x90a0b0c0d0e0f000}
			c.enqueue(&b, dest)
			n := binary.LittleEndian.Uint32(c.out)
			if n != uint32(16+len(payload)) || len(c.out) != 4+width+int(n) {
				t.Fatalf("bad frame length: %x", c.out)
			}
			packet := c.out[4+width:]
			if binary.LittleEndian.Uint64(packet) != dest.First || binary.LittleEndian.Uint64(packet[8:]) != dest.Second || !bytes.Equal(packet[16:], payload) {
				t.Fatalf("bad packet: %x", packet)
			}
			if width == 4 {
				// Independent bit-at-a-time Castagnoli reference, including the UID.
				crc := ^uint32(0)
				for _, v := range packet {
					crc ^= uint32(v)
					for range 8 {
						mask := uint32(0) - (crc & 1)
						crc = crc>>1 ^ (0x82f63b78 & mask)
					}
				}
				if got := binary.LittleEndian.Uint32(c.out[4:]); got != ^crc {
					t.Fatalf("CRC32C: got %08x, want %08x", got, ^crc)
				}
			}
		})
	}
}

func TestBinary62Negotiation(t *testing.T) {
	fs := newFakeServer(t, nil)
	fs.version = uint64(wire.Protocol62) // USE_OBJECT_SERIALIZER=0
	fs.start()
	c := connect(t, fs)
	if c.UsesObjectSerializer() || c.ProtocolVersion() != wire.Protocol62 || c.checksumBytes != 4 {
		t.Fatal("6.2 binary peer did not retain its selected codec and framing")
	}
	if err := call(c, echoTok); err != nil {
		t.Fatal(err)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.hellos) != 2 || fs.hellos[0] != uint64(wire.Protocol74)|objectSerializerFlag || fs.hellos[1] != uint64(wire.Protocol62) {
		t.Fatalf("binary advertisements: %x", fs.hellos)
	}
}

func TestOldObjectRemoteError(t *testing.T) {
	for _, version := range []wire.ProtocolVersion{wire.Protocol62, wire.Protocol63, wire.Protocol70} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			fs := newFakeServer(t, func(s *serverConn, dest wire.UID, msg []byte) {
				if dest == errTok {
					reply, err := wire.PingReplyToken(msg, s.protocol)
					if err != nil {
						t.Error(err)
						return
					}
					s.send(reply, errorReply(fdberr.FutureVersion))
					return
				}
				answerPings(s, dest, msg)
			})
			fs.version = uint64(version) | objectSerializerFlag
			fs.start()
			c := connect(t, fs)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if _, err := Call(ctx, c, errTok, wire.Ping{}); err != fdberr.FutureVersion {
				t.Fatalf("versioned ErrorOr identifier: %v", err)
			}
			if _, err := Call(ctx, c, echoTok, wire.Ping{}); err != nil {
				t.Fatalf("Void reply after remote error: %v", err)
			}
		})
	}
}

// The old notification serialized Endpoint (including its addresses), not UID.
func oldNotice(b *wire.Builder, protocol wire.ProtocolVersion, tok wire.UID, object bool) []byte {
	if !object {
		n := 8
		if protocol.HasIPv6() {
			n += 2 // IPv6 discriminator and absent secondary-address discriminator.
		}
		msg := b.Raw(n + 16)
		clear(msg)
		binary.LittleEndian.PutUint64(msg[n:], tok.First)
		binary.LittleEndian.PutUint64(msg[n+8:], tok.Second)
		return b.Finished()
	}
	ip := b.ScalarU32(0x7f000001)
	b.StartTable(2)
	b.AddUnion(0, 1, ip)
	ipt := b.EndTable()
	b.StartTable(4)
	b.AddOffset(0, ipt)
	b.AddU16(1, 4500)
	addr := b.EndTable()
	b.StartTable(3)
	b.AddOffset(0, addr)
	list := b.EndTable()
	b.StartTable(2)
	b.AddOffset(0, list)
	b.AddUID(1, tok)
	ep := b.EndTable()
	return b.FinishRequest(ep, 0)
}

func TestOldEndpointNotFound(t *testing.T) {
	for _, version := range []wire.ProtocolVersion{wire.Protocol50, wire.Protocol51, wire.Protocol52, wire.Protocol60, wire.Protocol61, wire.Protocol62, wire.Protocol63} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			fs := newFakeServer(t, func(s *serverConn, dest wire.UID, msg []byte) {
				if dest == goneTok {
					s.send(wire.WellKnown(wire.TokenEndpointNotFound), func(b *wire.Builder) []byte {
						return oldNotice(b, version, goneTok, s.object)
					})
					return
				}
				answerPings(s, dest, msg)
			})
			fs.version = uint64(version)
			if version.UsesObjectSerializer() {
				fs.version |= objectSerializerFlag
			}
			fs.start()
			c := connect(t, fs)
			if err := call(c, goneTok); err != EndpointNotFound {
				t.Fatalf("old endpoint notice: %v", err)
			}
			if err := call(c, echoTok); err != nil {
				t.Fatalf("notice failed entire connection: %v", err)
			}
		})
	}
}

func TestLegacyMalformedAndCancellation(t *testing.T) {
	for _, version := range []wire.ProtocolVersion{wire.Protocol50, wire.Protocol61, wire.Protocol62} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			requests := make(chan struct{}, 1)
			fs := newFakeServer(t, func(s *serverConn, dest wire.UID, msg []byte) {
				if dest == silentTok {
					requests <- struct{}{}
					return
				}
				if dest == errTok {
					reply, err := wire.PingReplyTokenLegacy(msg, s.protocol)
					if err == nil {
						s.send(reply, func(b *wire.Builder) []byte {
							copy(b.Raw(1), []byte{2}) // Invalid ErrorOr<bool> success tag.
							return b.Finished()
						})
					}
					return
				}
				answerPings(s, dest, msg)
			})
			fs.version = uint64(version) // Also exercises binary 6.2.
			fs.start()
			c := connect(t, fs)
			if err := call(c, errTok); err != Malformed {
				t.Fatalf("invalid legacy ErrorOr: %v", err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			result := make(chan error, 1)
			go func() { _, err := Call(ctx, c, silentTok, wire.Ping{}); result <- err }()
			select {
			case <-requests:
			case <-time.After(5 * time.Second):
				t.Fatal("legacy request was not received")
			}
			cancel()
			if err := <-result; err != context.Canceled {
				t.Fatalf("cancel: %v", err)
			}
			if !c.idle() {
				t.Fatal("canceled request retained its reply slot")
			}
			if err := call(c, echoTok); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLegacyBadFramesFailOutstandingRequest(t *testing.T) {
	for _, version := range []wire.ProtocolVersion{wire.Protocol50, wire.Protocol61, wire.Protocol63} {
		for _, length := range []uint32{15, packetLimit + 1, 16} {
			t.Run(fmt.Sprintf("%x/%d", version, length), func(t *testing.T) {
				fs := newFakeServer(t, func(s *serverConn, _ wire.UID, _ []byte) {
					s.mu.Lock()
					defer s.mu.Unlock()
					var hdr [8]byte
					binary.LittleEndian.PutUint32(hdr[:], length)
					s.w.Write(hdr[:]) // Zero is not the CRC32C of sixteen zero bytes.
					if length == 16 {
						s.w.Write(make([]byte, 16))
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
				if _, err := Call(ctx, c, echoTok, wire.Ping{}); err != MaybeDelivered {
					t.Fatalf("malformed frame must fail outstanding RPC: %v", err)
				}
				<-c.Done()
			})
		}
	}
}

func TestNoApplicationBytesOnMismatchedHandshake(t *testing.T) {
	for _, version := range []wire.ProtocolVersion{wire.Protocol50, wire.Protocol62, wire.Protocol63, wire.Protocol80} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			fs := newFakeServer(t, nil)
			fs.version = uint64(version)
			if version.UsesObjectSerializer() {
				fs.version |= objectSerializerFlag
			}
			fs.mismatchBytes = make(chan int, 1)
			fs.start()
			c := connect(t, fs)
			if err := call(c, echoTok); err != nil {
				t.Fatal(err)
			}
			if n := <-fs.mismatchBytes; n != 0 {
				t.Fatalf("sent %d application bytes before compatible handshake", n)
			}
		})
	}
}

func TestAnswersOldServerPings(t *testing.T) {
	for _, version := range []wire.ProtocolVersion{wire.Protocol50, wire.Protocol61, wire.Protocol62, wire.Protocol63} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			replyToken := wire.UID{First: 77, Second: 88}
			answered := make(chan struct{}, 1)
			fs := newFakeServer(t, func(s *serverConn, dest wire.UID, msg []byte) {
				if dest == replyToken {
					var err error
					if s.object {
						_, err = wire.DecodeReply(wire.Ping{}, msg, version)
					} else {
						_, err = wire.DecodeLegacy(wire.Ping{}, msg, version)
					}
					if err == nil {
						answered <- struct{}{}
					}
					return
				}
				answerPings(s, dest, msg)
			})
			fs.version = uint64(version)
			if version.UsesObjectSerializer() {
				fs.version |= objectSerializerFlag
			}
			fs.start()
			c := connect(t, fs)
			if err := call(c, echoTok); err != nil {
				t.Fatal(err)
			}
			fs.mu.Lock()
			s := fs.conns[len(fs.conns)-1]
			fs.mu.Unlock()
			s.send(wire.WellKnown(wire.TokenPing), func(b *wire.Builder) []byte {
				if s.object {
					off := wire.Ping{}.Encode(b, replyToken)
					return b.FinishRequest(off, wire.MessageFileID(wire.Ping{}, version))
				}
				if err := wire.EncodeLegacy(b, wire.Ping{}, replyToken, version); err != nil {
					panic(err)
				}
				return b.Finished()
			})
			if err := s.flush(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-answered:
			case <-time.After(5 * time.Second):
				t.Fatal("old server ping was not answered")
			}
		})
	}
}

type failingLegacyRequest struct{ wire.Ping }

func (failingLegacyRequest) EncodeLegacy(*wire.BinaryWriter, wire.UID) error {
	return fdberr.UnsupportedOperation
}

func TestLegacyEncodeFailureReleasesReplySlots(t *testing.T) {
	fs := newFakeServer(t, nil)
	fs.version = uint64(wire.Protocol61)
	fs.start()
	c := connect(t, fs)
	if _, err := Call(t.Context(), c, echoTok, failingLegacyRequest{}); err != fdberr.UnsupportedOperation {
		t.Fatalf("binary encode error: %v", err)
	}
	replies := CallBatch(t.Context(), []BatchCall[struct{}]{
		{Connection: c, Destination: echoTok, Request: failingLegacyRequest{}},
		{Connection: c, Destination: echoTok, Request: wire.Ping{}},
	})
	if replies[0].Err != fdberr.UnsupportedOperation || replies[1].Err != nil {
		t.Fatalf("mixed encode-error batch: %+v", replies)
	}
	if !c.idle() {
		t.Fatal("binary encode failure retained a reply slot")
	}
}

type unsupportedRequest struct{ wire.Ping }

func (unsupportedRequest) ValidateProtocol(wire.ProtocolVersion) error {
	return fdberr.UnsupportedOperation
}

func TestUnsupportedRequestNeverRegistersOrQueues(t *testing.T) {
	fs := newFakeServer(t, nil).start()
	c := connect(t, fs)
	before := c.nextID.Load()
	if _, err := Call(t.Context(), c, echoTok, unsupportedRequest{}); err != fdberr.UnsupportedOperation {
		t.Fatalf("unsupported Call: %v", err)
	}
	replies := CallBatch(t.Context(), []BatchCall[struct{}]{{Connection: c, Destination: echoTok, Request: unsupportedRequest{}}})
	if len(replies) != 1 || replies[0].Err != fdberr.UnsupportedOperation {
		t.Fatalf("unsupported batch: %+v", replies)
	}
	if c.nextID.Load() != before || !c.idle() {
		t.Fatal("unsupported request acquired a reply slot")
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if len(c.out) != 0 {
		t.Fatal("unsupported request queued application bytes")
	}
}
