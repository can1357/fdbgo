package transport

// Process-wide network options (fdb_network_set_option).
//
// libfdb_c applies these to its single network thread before it starts; here
// they configure every database opened afterwards. TLS options (and the
// FDB_TLS_* environment variables) select TLS for connections to :tls
// addresses. Options that configure machinery this client does not have (the
// multi-version client and external client libraries, client threads,
// run-loop profiling, buggify, trace files) are validated and accepted for
// compatibility and have no effect.

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/options"
)

// networkState holds the settings shared by all databases of the process.
type networkState struct {
	once sync.Once
	mu   sync.RWMutex
	tls  tlsConfig
	// client is replaced atomically after option changes or a successful file reload.
	client *tls.Config
	reload tlsReloadState
}

var network networkState

// networkConfig returns the process network settings, reading the
// environment on first use.
func networkConfig() *networkState {
	network.once.Do(func() { network.tls = tlsConfigFromEnv() })
	return &network
}

// optionParam is the parameter kind of a network option.
type optionParam int

const (
	paramNone optionParam = iota
	paramString
	paramInt
	paramBytes
)

// networkParam returns the parameter kind of a network option code; ok is
// false for codes this client does not know.
func networkParam(code int) (optionParam, bool) {
	switch code {
	case options.NetworkLocalAddress, options.NetworkClusterFile, options.NetworkTraceEnable,
		options.NetworkTraceLogGroup, options.NetworkTraceFormat, options.NetworkTraceClockSource,
		options.NetworkTraceFileIdentifier, options.NetworkTracePartialFileSuffix, options.NetworkKnob,
		options.NetworkTLSPlugin, options.NetworkTLSCertPath, options.NetworkTLSKeyPath,
		options.NetworkTLSCaPath, options.NetworkTLSPassword, options.NetworkExternalClientLibrary,
		options.NetworkExternalClientDirectory, options.NetworkFutureVersionClientLibrary,
		options.NetworkDistributedClientTracer, options.NetworkClientTmpDir:
		return paramString, true
	case options.NetworkTraceRollSize, options.NetworkTraceMaxLogsSize,
		options.NetworkBuggifySectionActivatedProbability, options.NetworkBuggifySectionFiredProbability,
		options.NetworkClientThreadsPerVersion, options.NetworkClientBuggifySectionActivatedProbability,
		options.NetworkClientBuggifySectionFiredProbability:
		return paramInt, true
	case options.NetworkTLSCertBytes, options.NetworkTLSKeyBytes, options.NetworkTLSVerifyPeers,
		options.NetworkTLSCaBytes:
		return paramBytes, true
	case options.NetworkTraceShareAmongClientThreads, options.NetworkTraceInitializeOnSetup,
		options.NetworkBuggifyEnable, options.NetworkBuggifyDisable,
		options.NetworkTLSDisablePlaintextConnection, options.NetworkDisableMultiVersionClientAPI,
		options.NetworkCallbacksOnExternalThreads, options.NetworkDisableLocalClient,
		options.NetworkRetainClientLibraryCopies, options.NetworkIgnoreExternalClientFailures,
		options.NetworkFailIncompatibleClient, options.NetworkDisableClientStatisticsLogging,
		options.NetworkEnableRunLoopProfiling, options.NetworkDisableClientBypass,
		options.NetworkClientBuggifyEnable, options.NetworkClientBuggifyDisable:
		return paramNone, true
	}
	return 0, false
}

// SetNetworkOption sets a network option for this process, like
// fdb_network_set_option: code is the raw option code, param its value
// (strings as bytes, integers as 8-byte little endian). Errors are
// invalid_option for unknown codes and invalid_option_value for malformed
// values.
func SetNetworkOption(code int, param []byte) error {
	kind, ok := networkParam(code)
	if !ok {
		return fdberr.InvalidOption
	}
	switch kind {
	case paramString:
		if !utf8.Valid(param) {
			return fdberr.InvalidOptionValue
		}
	case paramInt:
		if len(param) != 8 {
			return fdberr.InvalidOptionValue
		}
	}
	n := networkConfig()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.tls.applyNetworkOption(code, param) {
		n.client = nil
		return nil
	}
	if code == options.NetworkKnob {
		name, value, ok := strings.Cut(string(param), "=")
		if !ok {
			return fdberr.InvalidOptionValue
		}
		slog.Debug("FoundationDB client ignoring unrecognized knob option", "knob", name, "value", value)
	}
	return nil
}

// TLSEnabled reports whether TLS material is configured, so :tls addresses should be preferred.
func TLSEnabled() bool {
	n := networkConfig()
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.tls.isConfigured()
}

// PlaintextDisabled reports whether dialing plaintext addresses is refused
// (TLS_DISABLE_PLAINTEXT_CONNECTION).
func PlaintextDisabled() bool {
	n := networkConfig()
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.tls.disablePlaintext()
}

// tlsConnect runs the client TLS handshake on a connected socket. Errors are
// the TLS material's loading error (tls_error, file errors) or
// connection_failed when the handshake fails (including a server rejected by
// verify-peers).
func tlsConnect(ctx context.Context, raw net.Conn) (*tls.Conn, error) {
	cfg, err := networkConfig().tlsClientConfig(time.Now())
	if err != nil {
		return nil, err
	}
	tc := tls.Client(raw, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		slog.Debug("TLS handshake failed", "error", err)
		return nil, fdberr.ConnectionFailed
	}
	return tc, nil
}
