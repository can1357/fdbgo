package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
)

// replaceTLSReloadFile preserves file size and modification time. File identity
// may also be reused, so refresh must inspect the current material.
func replaceTLSReloadFile(t *testing.T, path string, material []byte) {
	t.Helper()
	contents := bytes.Repeat([]byte{'\n'}, 4096)
	if len(material) > len(contents) {
		t.Fatal("TLS test material exceeds padded file size")
	}
	copy(contents, material)
	replacement := path + ".replacement"
	if err := os.WriteFile(replacement, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(1700000000, 0)
	if err := os.Chtimes(replacement, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
}

// refreshNetworkTLS advances directly to the next production polling deadline.
// Actual handshakes still consume the process cache through tlsConnect.
func refreshNetworkTLS(t *testing.T) time.Time {
	t.Helper()
	n := networkConfig()
	n.mu.RLock()
	at := n.reload.nextCheck
	n.mu.RUnlock()
	if at.IsZero() {
		t.Fatal("file-backed TLS configuration has no refresh scheduled")
	}
	if _, err := n.tlsClientConfig(at); err != nil {
		t.Fatal(err)
	}
	return at
}

func requireTLSRefreshDelay(t *testing.T, at time.Time, delay time.Duration) {
	t.Helper()
	n := networkConfig()
	n.mu.RLock()
	next := n.reload.nextCheck
	n.mu.RUnlock()
	if got := next.Sub(at); got != delay {
		t.Fatalf("next TLS refresh after %s, want %s", got, delay)
	}
}

// handshakeReloadedTLS returns the client identity seen by a real TLS server.
// Both sides finish authentication before the result is returned.
func handshakeReloadedTLS(t *testing.T, server tls.Certificate, clientCA *x509.Certificate) (string, error) {
	t.Helper()
	clientRaw, serverRaw := net.Pipe()
	defer clientRaw.Close()
	defer serverRaw.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	serverConfig := &tls.Config{
		Certificates:           []tls.Certificate{server},
		SessionTicketsDisabled: true,
	}
	if clientCA != nil {
		pool := x509.NewCertPool()
		pool.AddCert(clientCA)
		serverConfig.ClientAuth = tls.RequireAndVerifyClientCert
		serverConfig.ClientCAs = pool
	}
	type result struct {
		identity string
		err      error
	}
	results := make(chan result, 1)
	go func() {
		tc := tls.Server(serverRaw, serverConfig)
		err := tc.HandshakeContext(ctx)
		identity := ""
		if err == nil {
			if peers := tc.ConnectionState().PeerCertificates; len(peers) != 0 {
				identity = peers[0].Subject.CommonName
			}
		}
		results <- result{identity: identity, err: err}
	}()
	_, err := tlsConnect(ctx, clientRaw)
	if err != nil {
		clientRaw.Close()
	}
	res := <-results
	if err != nil {
		return "", err
	}
	return res.identity, res.err
}

func requireReloadedTLSIdentity(t *testing.T, server tls.Certificate, ca *x509.Certificate, want string) {
	t.Helper()
	got, err := handshakeReloadedTLS(t, server, ca)
	if err != nil || got != want {
		t.Fatalf("server authenticated client %q, want %q: %v", got, want, err)
	}
}

func TestNetworkTLSReloadsCertificateAndKey(t *testing.T) {
	ca := rootCA(t, "TLS Reload Root")
	serverCert, serverKey := ca.sign(t, leafTemplate(t, leafName()))
	server := tls.Certificate{Certificate: [][]byte{serverCert.Raw}, PrivateKey: serverKey}
	oldCert, oldKey := ca.sign(t, leafTemplate(t, setAttr(leafName(), utf8Attr(oidCN, "client-before"))))
	newCert, newKey := ca.sign(t, leafTemplate(t, setAttr(leafName(), utf8Attr(oidCN, "client-after"))))

	for _, source := range []string{"option", "environment", "default"} {
		for _, first := range []string{"certificate", "key"} {
			t.Run(source+"/"+first+"-first", func(t *testing.T) {
				dir := t.TempDir()
				certPath := filepath.Join(dir, defaultCertFileName)
				keyPath := filepath.Join(dir, defaultKeyFileName)
				replaceTLSReloadFile(t, certPath, certPEM(oldCert))
				replaceTLSReloadFile(t, keyPath, keyPEM(t, oldKey))
				vars := map[string]string{}
				defaults := ""
				if source == "environment" {
					vars[envCertificateFile], vars[envKeyFile] = certPath, keyPath
				}
				if source == "default" {
					defaults = dir
				}
				c := tlsConfigFromEnvWith(envOf(vars), defaults)
				// Implicit files must outrank even a valid explicit byte pair.
				c.setCertBytes(certPEM(newCert))
				c.setKeyBytes(keyPEM(t, newKey))
				if source == "option" {
					c.setCertPath(certPath)
					c.setKeyPath(keyPath)
				}
				c.setCABytes(ca.pem())
				withNetworkTLS(t, c)
				requireReloadedTLSIdentity(t, server, ca.cert, "client-before")

				if first == "certificate" {
					replaceTLSReloadFile(t, certPath, certPEM(newCert))
				} else {
					replaceTLSReloadFile(t, keyPath, keyPEM(t, newKey))
				}
				// No file loading is attempted between polling deadlines.
				requireReloadedTLSIdentity(t, server, ca.cert, "client-before")
				failedAt := refreshNetworkTLS(t)
				requireTLSRefreshDelay(t, failedAt, tlsCertReloadRetryDelay)
				requireReloadedTLSIdentity(t, server, ca.cert, "client-before")

				if first == "certificate" {
					replaceTLSReloadFile(t, keyPath, keyPEM(t, newKey))
				} else {
					replaceTLSReloadFile(t, certPath, certPEM(newCert))
				}
				if _, err := networkConfig().tlsClientConfig(failedAt.Add(tlsCertReloadRetryDelay / 2)); err != nil {
					t.Fatal(err)
				}
				requireReloadedTLSIdentity(t, server, ca.cert, "client-before")
				loadedAt := refreshNetworkTLS(t)
				requireTLSRefreshDelay(t, loadedAt, tlsCertRefreshDelay)
				requireReloadedTLSIdentity(t, server, ca.cert, "client-after")
			})
		}
	}
}

func TestNetworkTLSReloadsCATrust(t *testing.T) {
	oldCA := rootCA(t, "TLS Old Root")
	newCA := rootCA(t, "TLS New Root")
	oldCert, oldKey := oldCA.sign(t, leafTemplate(t, leafName()))
	newCert, newKey := newCA.sign(t, leafTemplate(t, leafName()))
	oldServer := tls.Certificate{Certificate: [][]byte{oldCert.Raw}, PrivateKey: oldKey}
	newServer := tls.Certificate{Certificate: [][]byte{newCert.Raw}, PrivateKey: newKey}
	requireTrust := func(t *testing.T, server tls.Certificate, trusted bool) {
		t.Helper()
		_, err := handshakeReloadedTLS(t, server, nil)
		if trusted && err != nil {
			t.Fatalf("trusted server handshake: %v", err)
		}
		if !trusted && err != fdberr.ConnectionFailed {
			t.Fatalf("untrusted server handshake: %v, want connection_failed", err)
		}
	}
	for _, source := range []string{"option", "environment"} {
		t.Run(source, func(t *testing.T) {
			caPath := filepath.Join(t.TempDir(), "ca.pem")
			replaceTLSReloadFile(t, caPath, oldCA.pem())
			vars := map[string]string{}
			if source == "environment" {
				vars[envCAFile] = caPath
			}
			c := tlsConfigFromEnvWith(envOf(vars), "")
			c.setCABytes(newCA.pem())
			if source == "option" {
				c.setCAPath(caPath)
			}
			withNetworkTLS(t, c)
			requireTrust(t, oldServer, true)
			requireTrust(t, newServer, false)

			replaceTLSReloadFile(t, caPath, []byte("incomplete CA bundle"))
			failedAt := refreshNetworkTLS(t)
			requireTLSRefreshDelay(t, failedAt, tlsCertReloadRetryDelay)
			requireTrust(t, oldServer, true)
			requireTrust(t, newServer, false)

			replaceTLSReloadFile(t, caPath, newCA.pem())
			loadedAt := refreshNetworkTLS(t)
			requireTLSRefreshDelay(t, loadedAt, tlsCertRefreshDelay)
			requireTrust(t, newServer, true)
			// Successfully replacing trust must remove the old root, not merge it.
			requireTrust(t, oldServer, false)
		})
	}
}

func TestNetworkTLSReloadsNewDefaultFiles(t *testing.T) {
	ca := rootCA(t, "TLS Default Root")
	serverCert, serverKey := ca.sign(t, leafTemplate(t, leafName()))
	server := tls.Certificate{Certificate: [][]byte{serverCert.Raw}, PrivateKey: serverKey}
	oldCert, oldKey := ca.sign(t, leafTemplate(t, setAttr(leafName(), utf8Attr(oidCN, "client-bytes"))))
	newCert, newKey := ca.sign(t, leafTemplate(t, setAttr(leafName(), utf8Attr(oidCN, "client-default"))))
	dir := t.TempDir()
	c := tlsConfigFromEnvWith(envOf(nil), dir)
	c.setCertBytes(certPEM(oldCert))
	c.setKeyBytes(keyPEM(t, oldKey))
	c.setCABytes(ca.pem())
	withNetworkTLS(t, c)
	requireReloadedTLSIdentity(t, server, ca.cert, "client-bytes")

	replaceTLSReloadFile(t, filepath.Join(dir, defaultCertFileName), certPEM(newCert))
	replaceTLSReloadFile(t, filepath.Join(dir, defaultKeyFileName), keyPEM(t, newKey))
	// Let tlsConnect itself observe an expired polling deadline in this case.
	n := networkConfig()
	n.mu.Lock()
	n.reload.nextCheck = time.Unix(1, 0)
	n.mu.Unlock()
	requireReloadedTLSIdentity(t, server, ca.cert, "client-default")
}
