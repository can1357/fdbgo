package transport

import (
	"crypto/tls"
	"log/slog"
	"path/filepath"
	"time"
)

const (
	// tlsCertRefreshDelay matches TLS_CERT_REFRESH_DELAY_SECONDS in Flow.
	tlsCertRefreshDelay = 12 * time.Hour
	// Retry incomplete rotations without reloading on intervening handshakes.
	tlsCertReloadRetryDelay = time.Second
)

// tlsReloadState belongs to the process network configuration, not to a
// database. Polling happens on the first handshake after nextCheck, avoiding
// idle goroutines and filesystem work on intervening handshakes.
type tlsReloadState struct {
	nextCheck time.Time
}

func (r *tlsReloadState) due(now time.Time) bool {
	return !r.nextCheck.IsZero() && !now.Before(r.nextCheck)
}

// tlsWatchPaths mirrors loadPEM's path precedence. A default path is watched
// even when absent, since its later appearance takes precedence over bytes.
func (c *tlsConfig) tlsWatchPaths() [3]string {
	path := func(explicit *pemSource, envPath *string, defaultName string) string {
		switch {
		case explicit != nil && explicit.isPath && explicit.path != "":
			return explicit.path
		case envPath != nil:
			return *envPath
		case defaultName != "" && c.env.defaultConfigDir != "":
			return filepath.Join(c.env.defaultConfigDir, defaultName)
		default:
			return ""
		}
	}
	return [3]string{
		path(c.cert, c.env.certificateFile, defaultCertFileName),
		path(c.key, c.env.keyFile, defaultKeyFileName),
		path(c.ca, c.env.caFile, ""),
	}
}

// tlsClientConfig rereads material at each polling deadline: inode reuse and
// preserved timestamps cannot reliably identify a rotation. Failed reloads
// retain the last valid immutable configuration and retry after one second.
func (n *networkState) tlsClientConfig(now time.Time) (*tls.Config, error) {
	n.mu.RLock()
	cfg := n.client
	ready := cfg != nil && !n.reload.due(now)
	n.mu.RUnlock()
	if ready {
		return cfg, nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.client != nil && !n.reload.due(now) {
		return n.client, nil
	}

	cfg, err := n.tls.build()
	if err != nil {
		if n.client == nil {
			return nil, err
		}
		n.reload.nextCheck = now.Add(tlsCertReloadRetryDelay)
		slog.Debug("TLS certificate reload failed; retaining previous configuration", "error", err)
		return n.client, nil
	}
	n.client = cfg
	n.reload = tlsReloadState{}
	for _, path := range n.tls.tlsWatchPaths() {
		if path != "" {
			n.reload.nextCheck = now.Add(tlsCertRefreshDelay)
			break
		}
	}
	return cfg, nil
}
