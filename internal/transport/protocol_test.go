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
		if got := fs.hellos[i]; got != uint64(version)|objectSerializerFlag {
			t.Fatalf("handshake %d advertised %#x, want %#x with object serializer", i, got, version)
		}
	}
}

func TestConnectNegotiatesProtocol(t *testing.T) {
	for _, version := range []wire.ProtocolVersion{wire.Protocol74, wire.Protocol74 | 0x1234, wire.Protocol73, wire.Protocol73 | 0x5678} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			fs := newFakeServer(t, testHandler)
			fs.version = uint64(version) | objectSerializerFlag
			fs.start()
			c := connect(t, fs)
			if got := c.ProtocolVersion(); got != version {
				t.Fatalf("negotiated protocol: got %#x, want %#x", got, version)
			}
			if err := call(c, echoTok); err != nil {
				t.Fatalf("request after negotiation: %v", err)
			}
			if version.Compatible(wire.Protocol74) {
				checkAdvertisements(t, fs, wire.Protocol74)
			} else {
				checkAdvertisements(t, fs, wire.Protocol74, wire.Protocol73)
			}
		})
	}
}

func TestConnectionsKeepIndependentProtocols(t *testing.T) {
	versions := []wire.ProtocolVersion{wire.Protocol73, wire.Protocol74}
	calls := make([]BatchCall[struct{}], 0, len(versions))
	for _, version := range versions {
		fs := newFakeServer(t, testHandler)
		fs.version = uint64(version) | objectSerializerFlag
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
		0x0FDB00B072000000 | objectSerializerFlag,
		0x0FDB00B075000000 | objectSerializerFlag,
		uint64(wire.Protocol73) | 0x10000 | objectSerializerFlag,
		uint64(wire.Protocol74) | 0x10000 | objectSerializerFlag,
		uint64(wire.Protocol73),
		uint64(wire.Protocol74),
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
	for _, length := range []uint32{4, 1025} {
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
	for _, versions := range [][2]wire.ProtocolVersion{{wire.Protocol73, wire.Protocol74}, {wire.Protocol74, wire.Protocol73}} {
		t.Run(fmt.Sprintf("%x-to-%x", versions[0], versions[1]), func(t *testing.T) {
			received := make(chan struct{}, 2)
			fs := newFakeServer(t, func(s *serverConn, dest wire.UID, msg []byte) {
				if dest == silentTok {
					received <- struct{}{}
				}
				testHandler(s, dest, msg)
			})
			fs.version = uint64(versions[0]) | objectSerializerFlag
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
			fs.version = uint64(versions[1]) | objectSerializerFlag
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
			if versions[0] == wire.Protocol73 {
				checkAdvertisements(t, fs, wire.Protocol74, wire.Protocol73, wire.Protocol74)
			} else {
				checkAdvertisements(t, fs, wire.Protocol74, wire.Protocol74, wire.Protocol73)
			}
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
