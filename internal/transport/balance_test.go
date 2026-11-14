package transport

import (
	"context"
	"math"
	"net/netip"
	"testing"
	"testing/synctest"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/options"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestGrowBackoff(t *testing.T) {
	for _, tc := range [][2]float64{{0, 0.01}, {0.01, 0.02}, {1, 2}, {3, 5}, {5, 5}} {
		if got := GrowBackoff(tc[0]); got != tc[1] {
			t.Errorf("GrowBackoff(%v) = %v, want %v", tc[0], got, tc[1])
		}
	}
}

func TestSmootherFolds(t *testing.T) {
	s := NewSmoother(2)
	s.Reset(10)
	if s.estimate != 10 || s.total != 10 {
		t.Fatal("reset")
	}
	s.total = 20
	s.update(2) // one e-folding time
	want := 10 + 10*(1-math.Exp(-1))
	if math.Abs(s.estimate-want) > 1e-12 {
		t.Fatalf("estimate %v want %v", s.estimate, want)
	}
	s.update(2) // no elapsed time, no change
	if math.Abs(s.estimate-want) > 1e-12 {
		t.Fatal("changed without elapsed time")
	}
	s.SetTotal(5)
	if s.total != 5 || s.SmoothRate() >= 0 {
		t.Fatalf("total %v rate %v", s.total, s.SmoothRate())
	}
}

func TestNoteTimeIsMonotonic(t *testing.T) {
	now := time.Now()
	NoteTime(now)
	a := LoopTime()
	NoteTime(now.Add(-time.Second))
	if LoopTime() != a {
		t.Fatal("loop time went backwards")
	}
	if a <= 0 || a > Now() {
		t.Fatalf("loop time %v, now %v", a, Now())
	}
}

func TestPlanPrefersLeastLoaded(t *testing.T) {
	m := NewQueueModel()
	all := func(int) bool { return true }
	tok := func(i int) uint64 { return uint64(100 + i) }
	if p := m.Plan(1, 1, tok, all); p.Best != 0 || p.Race {
		t.Fatalf("single replica: %+v", p)
	}
	time.Sleep(time.Millisecond) // let the smoother see the load
	var tickets []Ticket
	for range 50 {
		tickets = append(tickets, m.Start(tok(0)))
	}
	time.Sleep(10 * time.Millisecond)
	for range 20 {
		p := m.Plan(2, 2, tok, all)
		if p.Best != 1 || p.Next != 0 {
			t.Fatalf("loaded replica chosen: %+v", p)
		}
		if !p.Race || p.SecondDelay <= 0 || p.SecondDelay > 10*time.Millisecond {
			t.Fatalf("second request: %+v", p)
		}
	}
	for i := range tickets {
		tickets[i].Release()
		tickets[i].Release() // idempotent
	}
	if got := m.slot(tok(0)).outstanding.load(); got != 0 {
		t.Fatalf("outstanding %v after release", got)
	}
	healthyOnlyZero := func(i int) bool { return i == 0 }
	if p := m.Plan(2, 2, tok, healthyOnlyZero); p.Best != 0 {
		t.Fatalf("failed replica chosen: %+v", p)
	}
}

func TestCompletedRequestsContributeToReplicaLoad(t *testing.T) {
	for _, release := range []bool{false, true} {
		name := "end"
		if release {
			name = "release"
		}
		t.Run(name, func(t *testing.T) {
			m := NewQueueModel()
			var previous float64
			for range 3 {
				ticket := m.Start(1)
				s := m.slot(1)
				// Advance this slot's last fold without sleeping or changing the
				// process clock. The outstanding request spans one e-folding.
				s.smoothTime = Now() - smoothingSecs
				if release {
					ticket.Release()
				} else {
					m.End(&ticket, true, false, -1)
				}
				if estimate := s.estimate.load(); estimate <= previous {
					t.Fatalf("completed requests did not accumulate load: %v <= %v", estimate, previous)
				} else {
					previous = estimate
				}
				p := m.Plan(2, 2, func(i int) uint64 { return uint64(i + 1) }, func(int) bool { return true })
				if p.Best != 1 {
					t.Fatalf("next read chose busy replica after completed request: %+v", p)
				}
			}
		})
	}
}

func TestStartFoldsPreviousOutstandingTotal(t *testing.T) {
	m := NewQueueModel()
	first := m.Start(1)
	s := m.slot(1)
	// An infinitely old fold has converged exactly to the old total,
	// independent of scheduling or the process clock.
	s.smoothTime = math.Inf(-1)
	second := m.Start(1)
	if got := s.estimate.load(); got != 1 {
		t.Fatalf("start integrated new total instead of old total: %v, want 1", got)
	}
	first.Release()
	second.Release()
}

func TestEndBacksOffFutureVersion(t *testing.T) {
	m := NewQueueModel()
	all := func(int) bool { return true }
	tok := func(i int) uint64 { return uint64(200 + i) }
	tk := m.Start(tok(0))
	m.End(&tk, false, true, -1)
	tk.Release() // no-op after End
	s := m.slot(tok(0))
	if s.outstanding.load() != 0 || s.futureVersionBackoff != 2 || !s.backoffRaised.Load() {
		t.Fatalf("backoff %v raised %v", s.futureVersionBackoff, s.backoffRaised.Load())
	}
	for range 10 {
		if p := m.Plan(2, 2, tok, all); p.Best != 1 {
			t.Fatalf("backed-off replica chosen: %+v", p)
		}
	}
	tk = m.Start(tok(0))
	m.End(&tk, true, false, 3)
	if s.futureVersionBackoff != futureVersionInitialBackoff || s.backoffRaised.Load() || s.penalty.load() != 3 {
		t.Fatal("clean reply did not reset backoff / record penalty")
	}
	tk = m.Start(tok(0))
	if s.outstanding.load() != 3 {
		t.Fatalf("penalty-weighted outstanding %v", s.outstanding.load())
	}
	m.End(&tk, true, false, -1)
	if s.penalty.load() != 3 || s.outstanding.load() != 0 {
		t.Fatal("unreported penalty changed state")
	}
}

func TestSecondRequestBudget(t *testing.T) {
	m := NewQueueModel()
	if m.TakeSecondBudget() {
		t.Fatal("budget starts empty")
	}
	for range 21 {
		m.FirstReplied()
	}
	if !m.TakeSecondBudget() {
		t.Fatal("budget not grown")
	}
	if m.TakeSecondBudget() {
		t.Fatal("budget spent twice")
	}
	if got := m.secondMultiplier.load(); got != 1+SecondRequestMultiplierGrowth {
		t.Fatalf("multiplier %v", got)
	}
	m.FirstReplied()
	if got := m.secondMultiplier.load(); math.Abs(got-(1+SecondRequestMultiplierGrowth-SecondRequestMultiplierDecay)) > 1e-12 {
		t.Fatalf("multiplier %v", got)
	}
	for range 5000 {
		m.FirstReplied()
	}
	if m.secondBudget.load() != SecondRequestMaxBudget || m.secondMultiplier.load() != 1 {
		t.Fatal("budget/multiplier not saturated")
	}
}

func TestFailureMonitor(t *testing.T) {
	var f FailureMonitor
	addr := wire.NetAddr{Addr: netip.MustParseAddrPort("10.0.0.1:4500")}
	ep := wire.Endpoint{Addr: addr, Token: wire.UID{First: 1, Second: 2}}
	if f.Failed(ep, false) || f.AddressIsFailed(addr) {
		t.Fatal("fresh monitor reports failure")
	}
	f.AddressFailed(addr)
	if !f.Failed(ep, false) || !f.AddressIsFailed(addr) {
		t.Fatal("address failure not reported")
	}
	f.AddressOK(addr)
	if f.Failed(ep, false) || f.marked.Load() != 0 {
		t.Fatal("address failure not cleared")
	}
	f.EndpointNotFound(ep)
	if !f.Failed(ep, false) || f.AddressIsFailed(addr) {
		t.Fatal("endpoint failure")
	}
	other := ep
	other.Token.Second++
	if f.Failed(other, false) {
		t.Fatal("other endpoint failed")
	}
	if !f.OnlyEndpointFailed(ep, false) || f.OnlyEndpointFailed(other, false) {
		t.Fatal("endpoint-only failure not distinguished from another token")
	}
	f.AddressFailed(addr)
	if f.OnlyEndpointFailed(ep, false) {
		t.Fatal("failed address reported as an endpoint-only failure")
	}
	f.AddressOK(addr)
	if !f.OnlyEndpointFailed(ep, false) {
		t.Fatal("recovered address lost its retired endpoint")
	}
	// As in a long-running process: the last failure was long ago.
	f.newestAlternativesFailure, f.lastAlternativesFailureSkipTime = Now()-10, Now()-10
	d := f.AllAlternativesFailedDelay()
	if d < 45*time.Millisecond || d > 55*time.Millisecond {
		t.Fatalf("first delay %v", d)
	}
	if d := f.AllAlternativesFailedDelay(); d < 45*time.Millisecond || d > 55*time.Millisecond {
		t.Fatalf("second delay %v", d)
	}
}

func TestFailureMonitorRecoveryWakesPromptly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var f FailureMonitor
		ep := wire.Endpoint{Addr: wire.NetAddr{Addr: netip.MustParseAddrPort("10.0.0.1:4500")}}
		f.AddressFailed(ep.Addr)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- f.WaitForRecovery(ctx, 1, func(int) wire.Endpoint { return ep }, false, time.Second)
		}()
		synctest.Wait() // The waiter has checked failure and subscribed.
		start := time.Now()
		f.AddressOK(ep.Addr)
		synctest.Wait()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("recovered endpoint: %v", err)
			}
		default:
			t.Fatal("AddressOK did not wake the recovery waiter")
		}
		if time.Since(start) != 0 {
			t.Fatal("recovery waited for a timer instead of notification")
		}
		// A recovery before subscription must not be lost either.
		if err := f.WaitForRecovery(ctx, 1, func(int) wire.Endpoint { return ep }, false, time.Second); err != nil {
			t.Fatalf("already recovered endpoint: %v", err)
		}
	})
}

func TestFailureMonitorRecoveryDeadlineAndCancellation(t *testing.T) {
	for _, mode := range []string{"persistent", "deadline", "cancel", "permanent", "repeated"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var f FailureMonitor
				ep := wire.Endpoint{Addr: wire.NetAddr{Addr: netip.MustParseAddrPort("10.0.0.1:4500")}}
				f.AddressFailed(ep.Addr)
				delay := 6 * time.Second
				if mode == "deadline" {
					delay = 10 * time.Millisecond
				}
				if mode == "permanent" {
					f.EndpointNotFound(ep)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				start := time.Now()
				go func() {
					done <- f.WaitForRecovery(ctx, 1, func(int) wire.Endpoint { return ep }, false, delay)
				}()
				synctest.Wait()
				wantDelay, want := delay, error(fdberr.AllAlternativesFailed)
				switch mode {
				case "cancel":
					cancel()
					wantDelay, want = 0, context.Canceled
				case "repeated":
					time.Sleep(time.Second)
					f.AddressFailed(ep.Addr)
				}
				if err := <-done; err != want {
					t.Fatalf("recovery = %v, want %v", err, want)
				}
				if elapsed := time.Since(start); elapsed != wantDelay {
					t.Fatalf("waited %v, want %v", elapsed, wantDelay)
				}
				if !f.AddressIsFailed(ep.Addr) || !f.Failed(ep, false) {
					t.Fatal("failed address recovered without a successful connection")
				}
			})
		})
	}
}

func TestSetNetworkOption(t *testing.T) {
	withNetworkTLS(t, tlsConfig{})
	for _, tc := range []struct {
		code  int
		param []byte
		want  error
	}{
		{9999, nil, fdberr.InvalidOption},
		{options.NetworkSupportedClientVersions, []byte("x"), fdberr.InvalidOption},
		{options.NetworkTraceRollSize, []byte{1, 2, 3}, fdberr.InvalidOptionValue},
		{options.NetworkTraceRollSize, make([]byte, 8), nil},
		{options.NetworkTraceEnable, []byte{0xff}, fdberr.InvalidOptionValue},
		{options.NetworkTraceEnable, nil, nil},
		{options.NetworkKnob, []byte("no_value"), fdberr.InvalidOptionValue},
		{options.NetworkKnob, []byte("min_trace_severity=10"), nil},
		{options.NetworkBuggifyEnable, nil, nil},
		{options.NetworkDisableMultiVersionClientAPI, nil, nil},
		{options.NetworkTLSDisablePlaintextConnection, nil, nil},
	} {
		if err := SetNetworkOption(tc.code, tc.param); err != tc.want {
			t.Errorf("option %d %q: got %v want %v", tc.code, tc.param, err, tc.want)
		}
	}
	if !PlaintextDisabled() || TLSEnabled() {
		t.Fatal("TLS options not applied")
	}
	if err := SetNetworkOption(options.NetworkTLSCaBytes, []byte("ca")); err != nil || !TLSEnabled() {
		t.Fatal("TLS CA bytes not applied", err)
	}
}
