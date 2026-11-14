package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestCallBatchOrdersMixedOutcomes(t *testing.T) {
	var tokens []wire.UID
	fs := newFakeServer(t, func(s *serverConn, dest wire.UID, msg []byte) {
		if dest == wire.WellKnown(wire.TokenPing) {
			answerPings(s, dest, msg)
			return
		}
		token, err := wire.PingReplyToken(msg)
		if err != nil {
			t.Error(err)
			return
		}
		tokens = append(tokens, token)
		if len(tokens) != 3 {
			return
		}
		// No reply until all three requests have reached the server, then
		// reverse their order and give each request a distinct outcome.
		s.send(tokens[2], func(*wire.Builder) []byte { return []byte{1} })
		s.send(tokens[1], errorReply(fdberr.FutureVersion))
		s.send(tokens[0], wire.EncodeVoidReply)
	}).start()
	c := connect(t, fs)
	other := connect(t, newFakeServer(t, testHandler).start())
	dead := connect(t, newFakeServer(t, testHandler).start())
	dead.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	calls := []BatchCall[struct{}]{
		{c, echoTok, wire.Ping{}},
		{dead, echoTok, wire.Ping{}},
		{other, goneTok, wire.Ping{}},
		{c, echoTok, wire.Ping{}},
		{other, echoTok, wire.Ping{}},
		{c, echoTok, wire.Ping{}},
		{other, deniedTok, wire.Ping{}},
	}
	replies := CallBatch(ctx, calls)
	want := []error{nil, NotDelivered, EndpointNotFound, fdberr.FutureVersion, nil, Malformed, Unauthorized}
	if len(replies) != len(want) {
		t.Fatalf("reply count = %d, want %d", len(replies), len(want))
	}
	for i, reply := range replies {
		if reply.Err != want[i] {
			t.Errorf("reply %d = %v, want %v", i, reply.Err, want[i])
		}
	}
	if !c.idle() || !other.idle() || !dead.idle() {
		t.Fatal("batch left registered reply slots")
	}
}

func TestCallBatchCancellationDropsLateReplies(t *testing.T) {
	type request struct {
		conn  *serverConn
		token wire.UID
	}
	requests := make(chan request, 3)
	fs := newFakeServer(t, func(s *serverConn, dest wire.UID, msg []byte) {
		if dest != silentTok {
			answerPings(s, dest, msg)
			return
		}
		token, err := wire.PingReplyToken(msg)
		if err != nil {
			t.Error(err)
			return
		}
		requests <- request{s, token}
	}).start()
	c := connect(t, fs)
	deadline, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	ctx, cancel := context.WithCancel(deadline)
	defer cancel()
	done := make(chan []BatchReply[struct{}], 1)
	go func() {
		done <- CallBatch(ctx, []BatchCall[struct{}]{
			{c, silentTok, wire.Ping{}},
			{c, silentTok, wire.Ping{}},
			{c, silentTok, wire.Ping{}},
		})
	}()
	pending := make([]request, 3)
	for i := range pending {
		select {
		case pending[i] = <-requests:
		case <-deadline.Done():
			t.Fatal("batch did not dispatch every request")
		}
	}
	// Complete the last slot while the collector is still blocked on the
	// first. A subsequent round trip proves its reply has been routed before
	// cancellation; collecting that removed slot must not lose its outcome.
	pending[2].conn.send(pending[2].token, wire.EncodeVoidReply)
	if err := pending[2].conn.flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := Call(deadline, c, echoTok, wire.Ping{}); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case replies := <-done:
		for i, reply := range replies {
			if i == 2 {
				if reply.Err != nil {
					t.Errorf("already delivered reply = %v", reply.Err)
				}
			} else if !errors.Is(reply.Err, context.Canceled) {
				t.Errorf("cancelled reply %d = %v", i, reply.Err)
			}
		}
	case <-deadline.Done():
		t.Fatal("batch did not settle cancellation")
	}
	if !c.idle() {
		t.Fatal("cancelled batch left reply slots")
	}
	for _, req := range pending[:2] {
		req.conn.send(req.token, wire.EncodeVoidReply)
		if err := req.conn.flush(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Call(deadline, c, echoTok, wire.Ping{}); err != nil {
		t.Fatalf("late replies poisoned the next request: %v", err)
	}
	if !c.idle() {
		t.Fatal("late replies recreated pending slots")
	}
}

func TestCallBatchConnectionFailureSettlesEveryRequest(t *testing.T) {
	requests := make(chan struct{}, 3)
	fs := newFakeServer(t, func(s *serverConn, dest wire.UID, msg []byte) {
		if dest == silentTok {
			requests <- struct{}{}
		} else {
			answerPings(s, dest, msg)
		}
	}).start()
	c := connect(t, fs)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan []BatchReply[struct{}], 1)
	go func() {
		done <- CallBatch(ctx, []BatchCall[struct{}]{
			{c, silentTok, wire.Ping{}},
			{c, silentTok, wire.Ping{}},
			{c, silentTok, wire.Ping{}},
		})
	}()
	for range 3 {
		select {
		case <-requests:
		case <-ctx.Done():
			t.Fatal("batch did not dispatch every request")
		}
	}
	c.Close()
	select {
	case replies := <-done:
		for i, reply := range replies {
			if reply.Err != MaybeDelivered {
				t.Errorf("disconnected reply %d = %v", i, reply.Err)
			}
		}
	case <-ctx.Done():
		t.Fatal("batch did not settle disconnection")
	}
	if !c.idle() {
		t.Fatal("failed connection retained batch slots")
	}
}
