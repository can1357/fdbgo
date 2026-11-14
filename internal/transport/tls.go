package transport

// TLS for FlowTransport connections, configured the way libfdb_c configures it.
//
// The certificate chain, private key, CA bundle, key password and
// verify-peers policy come from network options, then FDB_TLS_* environment
// variables, then cert.pem/key.pem in the platform config directory, with
// libfdb_c's precedence and TLS_* option semantics. Peers are checked against
// the verify-peers rules (Check.Valid, Check.Unexpired, S./I./R. name and
// subjectAltName criteria, | alternatives). As in libfdb_c, SNI is only sent
// for hostname connections, so dialing an IP address sends none.
//
// Deliberate differences from libfdb_c 7.4, each making a documented
// verify-peers feature work where libfdb_c ignores it:
//   - Check.Valid=0 skips chain validation as documented.
//   - Check.Unexpired=0 ignores validity periods for its rule (here: of
//     every certificate in the chain).
//   - subjectAltName=IP: matches the textual address (10.0.0.1, ::1).
//   - An escaped \| stays inside its expression.
//
// Chain validation uses crypto/x509 with the server-auth usage, which (unlike
// OpenSSL) rejects certificates whose extended key usage lacks serverAuth and
// SHA-1 signatures. A leaf that is itself one of the configured CA
// certificates is trusted directly, like OpenSSL does for a self-signed
// certificate used as both certificate and CA file.

import (
	"bytes"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"crypto/md5"
	"crypto/pbkdf2"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"io/fs"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/options"
)

const (
	// envCertificateFile overrides the certificate chain file.
	envCertificateFile = "FDB_TLS_CERTIFICATE_FILE"
	// envKeyFile overrides the private key file.
	envKeyFile = "FDB_TLS_KEY_FILE"
	// envCAFile overrides the CA bundle file.
	envCAFile = "FDB_TLS_CA_FILE"
	// envPassword is the private key passphrase fallback.
	envPassword = "FDB_TLS_PASSWORD"
	// envVerifyPeers is the verify-peers fallback.
	envVerifyPeers = "FDB_TLS_VERIFY_PEERS"
	// envDisablePlaintextConnection overrides plaintext refusal.
	envDisablePlaintextConnection = "FDB_TLS_DISABLE_PLAINTEXT_CONNECTION"
	// defaultCertFileName is looked up in the platform config directory when nothing else is set.
	defaultCertFileName = "cert.pem"
	// defaultKeyFileName is looked up in the platform config directory when nothing else is set.
	defaultKeyFileName = "key.pem"
	// defaultVerifyPeers is used when neither the option nor the environment provides one.
	defaultVerifyPeers = "Check.Valid=1"
	// certFileMaxSize is the largest certificate, key or CA file read, as in libfdb_c.
	certFileMaxSize = 5 * 1024 * 1024
)

// pemSource is where an explicitly configured PEM blob comes from: a file
// read at build time, or bytes used as-is.
type pemSource struct {
	isPath bool
	path   string
	bytes  []byte
}

// tlsEnv is a snapshot of the TLS environment variables and the platform config directory.
type tlsEnv struct {
	// certificateFile, keyFile and caFile are the FDB_TLS_*_FILE values, nil if unset (possibly empty).
	certificateFile, keyFile, caFile *string
	// password, verifyPeers and disablePlaintextConnection are nil if unset.
	password, verifyPeers, disablePlaintextConnection []byte
	// defaultConfigDir is the platform config directory, searched for cert.pem
	// and key.pem; "" for none.
	defaultConfigDir string
}

// tlsConfig holds client TLS settings with libfdb_c's precedence: explicit path >
// environment path > platform default file > explicit bytes for cert/key (no
// default file for the CA); explicit > environment for password and
// verify-peers.
type tlsConfig struct {
	// cert, key and ca are TLS_*_PATH / TLS_*_BYTES; setting one form clears the other.
	cert, key, ca *pemSource
	// password is TLS_PASSWORD; empty means unset.
	password []byte
	// verifyPeers holds the TLS_VERIFY_PEERS strings; empty means unset.
	verifyPeers [][]byte
	// disablePlaintextConnection is TLS_DISABLE_PLAINTEXT_CONNECTION.
	disablePlaintextConnection bool
	env                        tlsEnv
}

// String describes the configuration without printing key material.
func (c *tlsConfig) String() string {
	src := func(s *pemSource) string {
		switch {
		case s == nil:
			return "None"
		case s.isPath:
			return "Path(" + s.path + ")"
		}
		return fmt.Sprintf("Bytes(<%d bytes>)", len(s.bytes))
	}
	vp := make([]string, len(c.verifyPeers))
	for i, v := range c.verifyPeers {
		vp[i] = string(v)
	}
	return fmt.Sprintf("tlsConfig{cert: %s, key: %s, ca: %s, password_set: %t, verify_peers: %q, disable_plaintext_connection: %t}",
		src(c.cert), src(c.key), src(c.ca), len(c.password) > 0, vp, c.disablePlaintextConnection)
}

// tlsConfigFromEnv starts from the process environment (FDB_TLS_*) and the platform config directory.
func tlsConfigFromEnv() tlsConfig {
	return tlsConfigFromEnvWith(os.LookupEnv, platformDefaultConfigDir())
}

// tlsConfigFromEnvWith starts from the environment as seen through lookup,
// with defaultConfigDir for cert.pem/key.pem.
func tlsConfigFromEnvWith(lookup func(string) (string, bool), defaultConfigDir string) tlsConfig {
	str := func(name string) *string {
		if v, ok := lookup(name); ok {
			return &v
		}
		return nil
	}
	raw := func(name string) []byte {
		if v, ok := lookup(name); ok {
			return append([]byte{}, v...)
		}
		return nil
	}
	return tlsConfig{env: tlsEnv{
		certificateFile:            str(envCertificateFile),
		keyFile:                    str(envKeyFile),
		caFile:                     str(envCAFile),
		password:                   raw(envPassword),
		verifyPeers:                raw(envVerifyPeers),
		disablePlaintextConnection: raw(envDisablePlaintextConnection),
		defaultConfigDir:           defaultConfigDir,
	}}
}

func (c *tlsConfig) setCertPath(path string)    { c.cert = &pemSource{isPath: true, path: path} }
func (c *tlsConfig) setCertBytes(b []byte)      { c.cert = &pemSource{bytes: bytes.Clone(b)} }
func (c *tlsConfig) setKeyPath(path string)     { c.key = &pemSource{isPath: true, path: path} }
func (c *tlsConfig) setKeyBytes(b []byte)       { c.key = &pemSource{bytes: bytes.Clone(b)} }
func (c *tlsConfig) setCAPath(path string)      { c.ca = &pemSource{isPath: true, path: path} }
func (c *tlsConfig) setCABytes(b []byte)        { c.ca = &pemSource{bytes: bytes.Clone(b)} }
func (c *tlsConfig) setPassword(p []byte)       { c.password = bytes.Clone(p) }
func (c *tlsConfig) setDisablePlaintext(d bool) { c.disablePlaintextConnection = d }

// setVerifyPeers replaces earlier values (as in libfdb_c); | separates alternatives.
func (c *tlsConfig) setVerifyPeers(v []byte) { c.verifyPeers = [][]byte{bytes.Clone(v)} }

// applyNetworkOption applies a TLS_* network option; it returns false for
// options this file does not own. param has been validated for the option's type.
func (c *tlsConfig) applyNetworkOption(code int, param []byte) bool {
	switch code {
	case options.NetworkTLSPlugin:
	case options.NetworkTLSCertBytes:
		c.setCertBytes(param)
	case options.NetworkTLSCertPath:
		c.setCertPath(string(param))
	case options.NetworkTLSKeyBytes:
		c.setKeyBytes(param)
	case options.NetworkTLSKeyPath:
		c.setKeyPath(string(param))
	case options.NetworkTLSCaBytes:
		c.setCABytes(param)
	case options.NetworkTLSCaPath:
		c.setCAPath(string(param))
	case options.NetworkTLSPassword:
		c.setPassword(param)
	case options.NetworkTLSVerifyPeers:
		c.setVerifyPeers(param)
	case options.NetworkTLSDisablePlaintextConnection:
		c.setDisablePlaintext(true)
	default:
		return false
	}
	return true
}

// disablePlaintext reports whether plaintext connections are refused; a
// valid 0/1 environment value wins over the option.
func (c *tlsConfig) disablePlaintext() bool {
	if raw := c.env.disablePlaintextConnection; raw != nil {
		switch string(raw) {
		case "1":
			return true
		case "0":
			return false
		default:
			slog.Debug("Ignoring invalid "+envDisablePlaintextConnection, "value", string(raw))
		}
	}
	return c.disablePlaintextConnection
}

// isConfigured reports whether a certificate, key or CA is configured by
// option, FDB_TLS_* file variable, or platform default file.
func (c *tlsConfig) isConfigured() bool {
	explicit := func(s *pemSource) bool {
		if s == nil {
			return false
		}
		if s.isPath {
			return s.path != ""
		}
		return len(s.bytes) > 0
	}
	envFile := func(p *string) bool { return p != nil && *p != "" }
	if explicit(c.cert) || explicit(c.key) || explicit(c.ca) ||
		envFile(c.env.certificateFile) || envFile(c.env.keyFile) || envFile(c.env.caFile) {
		return true
	}
	if dir := c.env.defaultConfigDir; dir != "" {
		return isFile(filepath.Join(dir, defaultCertFileName)) || isFile(filepath.Join(dir, defaultKeyFileName))
	}
	return false
}

func isFile(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.Mode().IsRegular()
}

// build loads certificates and key and returns a client configuration that
// authenticates with them and verifies servers per verify-peers
// (ConfigureSSLContext + TLSPolicy). Errors are tls_error or the file errors
// of readPEMFile.
func (c *tlsConfig) build() (*tls.Config, error) {
	v, err := c.peerVerifier()
	if err != nil {
		return nil, err
	}
	certPEM, err := c.certificatePEM()
	if err != nil {
		return nil, err
	}
	keyPEM, err := c.keyPEM()
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		// The server certificate is checked by the verify-peers policy,
		// not by hostname: FDB dials IP addresses and sends no SNI.
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: v.verify,
	}
	switch {
	case certPEM != nil && keyPEM != nil:
		chain, err := parseCertificates(certPEM)
		if err != nil {
			return nil, err
		}
		key, err := parsePrivateKey(keyPEM, c.effectivePassword())
		if err != nil {
			return nil, err
		}
		pub, ok := chain[0].PublicKey.(interface{ Equal(crypto.PublicKey) bool })
		if !ok || !pub.Equal(key.Public()) {
			return nil, tlsError("private key does not match certificate")
		}
		cert := &tls.Certificate{PrivateKey: key, Leaf: chain[0]}
		for _, c := range chain {
			cert.Certificate = append(cert.Certificate, c.Raw)
		}
		// Present the certificate whatever the server asks for, like OpenSSL does.
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return cert, nil }
	case certPEM != nil:
		return nil, tlsError("TLS certificate configured without a private key")
	case keyPEM != nil:
		return nil, tlsError("TLS private key configured without a certificate")
	}
	return cfg, nil
}

// peerVerifier builds the server certificate verifier from the CA bundle and verify-peers rules.
func (c *tlsConfig) peerVerifier() (*peerVerifier, error) {
	rules, err := parseVerifyPeers(c.effectiveVerifyPeers())
	if err != nil {
		return nil, err
	}
	v := &peerVerifier{pool: x509.NewCertPool(), rules: rules}
	caPEM, err := c.caPEM()
	if err != nil {
		return nil, err
	}
	if caPEM != nil {
		if v.roots, err = parseCertificates(caPEM); err != nil {
			return nil, err
		}
		for _, r := range v.roots {
			v.pool.AddCert(r)
		}
	}
	return v, nil
}

// certificatePEM returns the resolved certificate chain PEM, nil if none.
func (c *tlsConfig) certificatePEM() ([]byte, error) {
	return c.loadPEM(c.cert, c.env.certificateFile, defaultCertFileName, "Certificate")
}

// keyPEM returns the resolved private key PEM, nil if none.
func (c *tlsConfig) keyPEM() ([]byte, error) {
	return c.loadPEM(c.key, c.env.keyFile, defaultKeyFileName, "Key")
}

// caPEM returns the resolved CA bundle PEM, nil if none.
func (c *tlsConfig) caPEM() ([]byte, error) {
	return c.loadPEM(c.ca, c.env.caFile, "", "CA")
}

// loadPEM loads one PEM item: a non-empty path (explicit,
// environment, existing default file) wins over bytes.
func (c *tlsConfig) loadPEM(explicit *pemSource, envPath *string, defaultName, what string) ([]byte, error) {
	path := ""
	switch {
	case explicit != nil && explicit.isPath && explicit.path != "":
		path = explicit.path
	case envPath != nil:
		path = *envPath
	case defaultName != "" && c.env.defaultConfigDir != "":
		if p := filepath.Join(c.env.defaultConfigDir, defaultName); isFile(p) {
			path = p
		}
	}
	var pem []byte
	switch {
	case path != "":
		var err error
		if pem, err = readPEMFile(path, what); err != nil {
			return nil, err
		}
	case explicit != nil && !explicit.isPath:
		pem = explicit.bytes
	default:
		return nil, nil
	}
	if len(pem) == 0 {
		return nil, nil
	}
	return pem, nil
}

// effectivePassword is the explicit password, else FDB_TLS_PASSWORD, else empty.
func (c *tlsConfig) effectivePassword() []byte {
	if len(c.password) > 0 {
		return c.password
	}
	return c.env.password
}

// effectiveVerifyPeers is the explicit verify-peers, else FDB_TLS_VERIFY_PEERS, else Check.Valid=1.
func (c *tlsConfig) effectiveVerifyPeers() [][]byte {
	switch {
	case len(c.verifyPeers) > 0:
		return c.verifyPeers
	case c.env.verifyPeers != nil:
		return [][]byte{c.env.verifyPeers}
	}
	return [][]byte{[]byte(defaultVerifyPeers)}
}

// platformDefaultConfigDir returns the platform config directory (as used by libfdb_c).
func platformDefaultConfigDir() string {
	switch runtime.GOOS {
	case "linux":
		return "/etc/foundationdb"
	case "darwin", "freebsd":
		return "/usr/local/etc/foundationdb"
	case "windows":
		if dir := os.Getenv("ProgramData"); dir != "" {
			return filepath.Join(dir, "foundationdb")
		}
	}
	return ""
}

// readPEMFile reads a file of at most certFileMaxSize bytes, failing with libfdb_c's error codes.
func readPEMFile(path, what string) ([]byte, error) {
	fail := func(err fdberr.Error) ([]byte, error) {
		slog.Debug("Error reading TLS "+what, "path", path, "error", err.Name())
		return nil, err
	}
	ioErr := func(err error) ([]byte, error) {
		if errors.Is(err, fs.ErrNotExist) {
			return fail(fdberr.FileNotFound)
		}
		return fail(fdberr.IOError)
	}
	st, err := os.Stat(path)
	if err != nil {
		return ioErr(err)
	}
	if st.Size() > certFileMaxSize {
		return fail(fdberr.FileTooLarge)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ioErr(err)
	}
	return b, nil
}

// tlsError logs a TLS configuration failure and returns tls_error, as ConfigureSSLContext does.
func tlsError(detail any) error {
	slog.Debug("TLSContextConfigureError", "detail", detail)
	return fdberr.TLSError
}

// parseCertificates parses every CERTIFICATE block of a PEM bundle; at least one is required.
func parseCertificates(pemBytes []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	text := string(pemBytes)
	for {
		label, body, rest, ok := nextPEMBlock(text)
		if !ok {
			break
		}
		text = rest
		if label != "CERTIFICATE" {
			continue
		}
		blk, ok := decodePEMBody(body)
		if !ok {
			return nil, tlsError("malformed certificate PEM")
		}
		cert, err := x509.ParseCertificate(blk.der)
		if err != nil {
			return nil, tlsError(err)
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return nil, tlsError("no certificate found in PEM data")
	}
	return certs, nil
}

// keyLabels are the PEM labels libfdb_c accepts for private keys, in the forms supported here.
var keyLabels = []string{"PRIVATE KEY", "ENCRYPTED PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY"}

// parsePrivateKey parses the first private key in pem, decrypting PKCS#8
// (ENCRYPTED PRIVATE KEY) or legacy OpenSSL (Proc-Type: 4,ENCRYPTED) keys with password.
func parsePrivateKey(pemBytes, password []byte) (crypto.Signer, error) {
	if !utf8.Valid(pemBytes) {
		return nil, tlsError("private key PEM is not UTF-8")
	}
	label, body, ok := findPEMBlock(string(pemBytes), keyLabels)
	if !ok {
		return nil, tlsError("no private key found in PEM data")
	}
	blk, ok := decodePEMBody(body)
	if !ok {
		return nil, tlsError("malformed private key PEM")
	}
	der := blk.der
	if blk.dekInfo != "" {
		if der, ok = decryptLegacyKey(blk.dekInfo, password, der); !ok {
			return nil, tlsError("cannot decrypt private key (wrong password or unsupported cipher)")
		}
	}
	var key any
	var err error
	switch label {
	case "PRIVATE KEY":
		key, err = x509.ParsePKCS8PrivateKey(der)
	case "RSA PRIVATE KEY":
		key, err = x509.ParsePKCS1PrivateKey(der)
	case "EC PRIVATE KEY":
		key, err = x509.ParseECPrivateKey(der)
	default:
		var plain []byte
		if plain, err = decryptPKCS8(der, password); err == nil {
			key, err = x509.ParsePKCS8PrivateKey(plain)
		}
	}
	if err != nil {
		return nil, tlsError(err)
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, tlsError("unsupported private key type")
	}
	return signer, nil
}

// nextPEMBlock finds the next "-----BEGIN label-----" ... "-----END label-----"
// block in text, returning its label, the text between the markers and the
// text after it.
func nextPEMBlock(text string) (label, body, rest string, ok bool) {
	begin := strings.Index(text, "-----BEGIN ")
	if begin < 0 {
		return "", "", "", false
	}
	after := text[begin+len("-----BEGIN "):]
	n := strings.Index(after, "-----")
	if n < 0 {
		return "", "", "", false
	}
	label = after[:n]
	body = after[n+len("-----"):]
	endMarker := "-----END " + label + "-----"
	end := strings.Index(body, endMarker)
	if end < 0 {
		return "", "", "", false
	}
	return label, body[:end], body[end+len(endMarker):], true
}

// findPEMBlock finds the first PEM block whose label is in labels,
// returning the label and the text between the markers.
func findPEMBlock(text string, labels []string) (label, body string, ok bool) {
	for {
		label, body, rest, ok := nextPEMBlock(text)
		if !ok {
			return "", "", false
		}
		for _, l := range labels {
			if l == label {
				return label, body, true
			}
		}
		text = rest
	}
}

// pemBody is a decoded PEM body with its RFC 1421 encryption header, if any.
type pemBody struct {
	// dekInfo is the DEK-Info value when Proc-Type: 4,ENCRYPTED is present.
	dekInfo string
	der     []byte
}

// decodePEMBody splits RFC 1421 headers from the base64 payload and decodes it.
func decodePEMBody(body string) (pemBody, bool) {
	encrypted := false
	dekInfo := ""
	var b64 strings.Builder
	for line := range strings.Lines(body) {
		line = strings.TrimSpace(line)
		name, value, isHeader := strings.Cut(line, ":")
		if isHeader && b64.Len() == 0 {
			switch strings.TrimSpace(name) {
			case "Proc-Type":
				encrypted = strings.TrimSpace(value) == "4,ENCRYPTED"
			case "DEK-Info":
				dekInfo = strings.TrimSpace(value)
			}
			continue
		}
		b64.WriteString(line)
	}
	der, err := base64.StdEncoding.DecodeString(b64.String())
	if err != nil {
		return pemBody{}, false
	}
	switch {
	case encrypted && dekInfo == "":
		return pemBody{}, false
	case encrypted:
		return pemBody{dekInfo: dekInfo, der: der}, true
	}
	return pemBody{der: der}, true
}

// decryptLegacyKey decrypts a legacy OpenSSL-encrypted key body (EVP_BytesToKey with MD5, CBC mode).
func decryptLegacyKey(dekInfo string, password, data []byte) ([]byte, bool) {
	name, ivHex, ok := strings.Cut(dekInfo, ",")
	if !ok {
		return nil, false
	}
	iv, ok := decodeHex(strings.TrimSpace(ivHex))
	if !ok {
		return nil, false
	}
	var keyLen, ivLen int
	var newCipher func([]byte) (cipher.Block, error)
	switch strings.TrimSpace(name) {
	case "AES-128-CBC":
		keyLen, ivLen, newCipher = 16, 16, aes.NewCipher
	case "AES-192-CBC":
		keyLen, ivLen, newCipher = 24, 16, aes.NewCipher
	case "AES-256-CBC":
		keyLen, ivLen, newCipher = 32, 16, aes.NewCipher
	case "DES-EDE3-CBC":
		keyLen, ivLen, newCipher = 24, 8, des.NewTripleDESCipher
	case "DES-CBC":
		keyLen, ivLen, newCipher = 8, 8, des.NewCipher
	default:
		return nil, false
	}
	if len(iv) != ivLen {
		return nil, false
	}
	key := evpBytesToKeyMD5(password, iv[:8], keyLen)
	block, err := newCipher(key)
	if err != nil {
		return nil, false
	}
	return cbcDecrypt(block, iv, data)
}

// cbcDecrypt CBC-decrypts data and strips PKCS#7 padding.
func cbcDecrypt(block cipher.Block, iv, data []byte) ([]byte, bool) {
	bs := block.BlockSize()
	if len(data) == 0 || len(data)%bs != 0 || len(iv) != bs {
		return nil, false
	}
	out := make([]byte, len(data))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(out, data)
	pad := int(out[len(out)-1])
	if pad == 0 || pad > bs {
		return nil, false
	}
	for _, b := range out[len(out)-pad:] {
		if int(b) != pad {
			return nil, false
		}
	}
	return out[:len(out)-pad], true
}

// evpBytesToKeyMD5 is OpenSSL EVP_BytesToKey(cipher, EVP_md5(), salt, password, count = 1).
func evpBytesToKeyMD5(password, salt []byte, keyLen int) []byte {
	key := make([]byte, 0, keyLen+md5.Size)
	var block []byte
	for len(key) < keyLen {
		h := md5.New()
		h.Write(block)
		h.Write(password)
		h.Write(salt)
		block = h.Sum(nil)
		key = append(key, block...)
	}
	return key[:keyLen]
}

// decodeHex decodes an even-length hex string, either case.
func decodeHex(s string) ([]byte, bool) {
	if len(s)%2 != 0 {
		return nil, false
	}
	out := make([]byte, len(s)/2)
	for i := range out {
		hi, ok1 := hexValue(s[2*i])
		lo, ok2 := hexValue(s[2*i+1])
		if !ok1 || !ok2 {
			return nil, false
		}
		out[i] = hi<<4 | lo
	}
	return out, true
}

// hexValue is hexValue: one hex digit, either case.
func hexValue(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// PKCS#5 / PKCS#8 encryption algorithm identifiers.
var (
	oidPBES2      = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 5, 13}
	oidPBKDF2     = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 5, 12}
	oidHMACSHA1   = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 7}
	oidHMACSHA224 = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 8}
	oidHMACSHA256 = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 9}
	oidHMACSHA384 = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 10}
	oidHMACSHA512 = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 11}
	oidAES128CBC  = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 1, 2}
	oidAES192CBC  = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 1, 22}
	oidAES256CBC  = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 1, 42}
	oidDESEDE3CBC = asn1.ObjectIdentifier{1, 2, 840, 113549, 3, 7}
	oidDESCBC     = asn1.ObjectIdentifier{1, 3, 14, 3, 2, 7}
)

type algorithmIdentifier struct {
	Algorithm  asn1.ObjectIdentifier
	Parameters asn1.RawValue `asn1:"optional"`
}

type encryptedPrivateKeyInfo struct {
	Algorithm     algorithmIdentifier
	EncryptedData []byte
}

type pbes2Params struct {
	KeyDerivationFunc algorithmIdentifier
	EncryptionScheme  algorithmIdentifier
}

type pbkdf2Params struct {
	Salt           []byte
	IterationCount int
	KeyLength      int                 `asn1:"optional"`
	PRF            algorithmIdentifier `asn1:"optional"`
}

// decryptPKCS8 decrypts an EncryptedPrivateKeyInfo protected by PBES2 with
// PBKDF2 (HMAC-SHA1/224/256/384/512) and AES-CBC, DES-EDE3-CBC or DES-CBC,
// returning the PrivateKeyInfo DER.
func decryptPKCS8(der, password []byte) ([]byte, error) {
	var info encryptedPrivateKeyInfo
	if rest, err := asn1.Unmarshal(der, &info); err != nil || len(rest) != 0 {
		return nil, errors.New("malformed EncryptedPrivateKeyInfo")
	}
	if !info.Algorithm.Algorithm.Equal(oidPBES2) {
		return nil, fmt.Errorf("unsupported PKCS#8 encryption %v", info.Algorithm.Algorithm)
	}
	var params pbes2Params
	if _, err := asn1.Unmarshal(info.Algorithm.Parameters.FullBytes, &params); err != nil {
		return nil, errors.New("malformed PBES2 parameters")
	}
	if !params.KeyDerivationFunc.Algorithm.Equal(oidPBKDF2) {
		return nil, fmt.Errorf("unsupported key derivation %v", params.KeyDerivationFunc.Algorithm)
	}
	var kdf pbkdf2Params
	if _, err := asn1.Unmarshal(params.KeyDerivationFunc.Parameters.FullBytes, &kdf); err != nil {
		return nil, errors.New("malformed PBKDF2 parameters")
	}
	var prf func() hash.Hash
	switch alg := kdf.PRF.Algorithm; {
	case len(alg) == 0 || alg.Equal(oidHMACSHA1):
		prf = sha1.New
	case alg.Equal(oidHMACSHA224):
		prf = sha256.New224
	case alg.Equal(oidHMACSHA256):
		prf = sha256.New
	case alg.Equal(oidHMACSHA384):
		prf = sha512.New384
	case alg.Equal(oidHMACSHA512):
		prf = sha512.New
	default:
		return nil, fmt.Errorf("unsupported PBKDF2 PRF %v", alg)
	}
	var keyLen int
	var newCipher func([]byte) (cipher.Block, error)
	switch alg := params.EncryptionScheme.Algorithm; {
	case alg.Equal(oidAES128CBC):
		keyLen, newCipher = 16, aes.NewCipher
	case alg.Equal(oidAES192CBC):
		keyLen, newCipher = 24, aes.NewCipher
	case alg.Equal(oidAES256CBC):
		keyLen, newCipher = 32, aes.NewCipher
	case alg.Equal(oidDESEDE3CBC):
		keyLen, newCipher = 24, des.NewTripleDESCipher
	case alg.Equal(oidDESCBC):
		keyLen, newCipher = 8, des.NewCipher
	default:
		return nil, fmt.Errorf("unsupported PBES2 cipher %v", alg)
	}
	if kdf.KeyLength != 0 && kdf.KeyLength != keyLen {
		return nil, errors.New("PBKDF2 key length does not match cipher")
	}
	var iv []byte
	if _, err := asn1.Unmarshal(params.EncryptionScheme.Parameters.FullBytes, &iv); err != nil {
		return nil, errors.New("malformed cipher IV")
	}
	if kdf.IterationCount <= 0 {
		return nil, errors.New("invalid PBKDF2 iteration count")
	}
	key, err := pbkdf2.Key(prf, string(password), kdf.Salt, kdf.IterationCount, keyLen)
	if err != nil {
		return nil, err
	}
	block, err := newCipher(key)
	if err != nil {
		return nil, err
	}
	plain, ok := cbcDecrypt(block, iv, info.EncryptedData)
	if !ok {
		return nil, errors.New("cannot decrypt private key (wrong password?)")
	}
	return plain, nil
}

// matchType is MatchType: how a criterion compares to the certificate value.
type matchType int

const (
	matchExact  matchType = iota // field=value
	matchPrefix                  // field>=value
	matchSuffix                  // field<=value
)

// certField is the certificate field a criterion inspects (abbrevToNID + locationForNID):
// a distinguished-name attribute, or the subject alternative name extension when san is set.
type certField struct {
	short string
	oid   asn1.ObjectIdentifier
	san   bool
}

// fieldFromAbbrev maps libfdb_c's accepted verify-peers abbreviations to fields.
func fieldFromAbbrev(abbrev []byte) (certField, bool) {
	switch string(abbrev) {
	case "C":
		return certField{short: "C", oid: asn1.ObjectIdentifier{2, 5, 4, 6}}, true
	case "CN":
		return certField{short: "CN", oid: asn1.ObjectIdentifier{2, 5, 4, 3}}, true
	case "L":
		return certField{short: "L", oid: asn1.ObjectIdentifier{2, 5, 4, 7}}, true
	case "ST":
		return certField{short: "ST", oid: asn1.ObjectIdentifier{2, 5, 4, 8}}, true
	case "O":
		return certField{short: "O", oid: asn1.ObjectIdentifier{2, 5, 4, 10}}, true
	case "OU":
		return certField{short: "OU", oid: asn1.ObjectIdentifier{2, 5, 4, 11}}, true
	case "UID":
		return certField{short: "UID", oid: asn1.ObjectIdentifier{0, 9, 2342, 19200300, 100, 1, 1}}, true
	case "DC":
		return certField{short: "DC", oid: asn1.ObjectIdentifier{0, 9, 2342, 19200300, 100, 1, 25}}, true
	case "subjectAltName":
		return certField{short: "subjectAltName", san: true}, true
	}
	return certField{}, false
}

// criterion is one field[<>]?=value condition.
type criterion struct {
	field     certField
	matchType matchType
	// raw is the unescaped value bytes.
	raw []byte
	// utf8 is raw read as Latin-1 and re-encoded as UTF-8, as libfdb_c does.
	utf8 string
}

func (c *criterion) String() string {
	op := "="
	switch c.matchType {
	case matchPrefix:
		op = ">="
	case matchSuffix:
		op = "<="
	}
	return c.field.short + op + c.utf8
}

func (c *criterion) equal(o *criterion) bool {
	return c.field.short == o.field.short && c.matchType == o.matchType && bytes.Equal(c.raw, o.raw)
}

// rule is one verify-peers alternative.
type rule struct {
	// subject, issuer and root hold criteria on the peer certificate's subject
	// (S./Subject./no prefix), its issuer name (I./Issuer.) and the trust
	// anchor's subject (R./Root.).
	subject, issuer, root []criterion
	// verifyCert is Check.Valid; verifyTime is Check.Unexpired.
	verifyCert, verifyTime bool
}

// parseRule parses one comma-separated expression.
func parseRule(input []byte) (rule, error) {
	r := rule{verifyCert: true, verifyTime: true}
	s := 0
	for s < len(input) {
		i := bytes.IndexByte(input[s:], '=')
		if i < 0 {
			return rule{}, errors.New("missing '='")
		}
		eq := s + i
		mt := matchExact
		if eq > 0 {
			switch input[eq-1] {
			case '>':
				mt = matchPrefix
			case '<':
				mt = matchSuffix
			}
		}
		termEnd := eq
		if mt != matchExact {
			termEnd = eq - 1
		}
		term := input[s:max(termEnd, s)]

		if bytes.HasPrefix(term, []byte("Check.")) {
			if eq+2 > len(input) {
				return rule{}, errors.New("missing Check value")
			}
			if eq+2 != len(input) && input[eq+2] != ',' {
				return rule{}, errors.New("Check value must be a single digit")
			}
			if mt != matchExact {
				return rule{}, errors.New("cannot prefix or suffix match Check")
			}
			var flag *bool
			switch string(term) {
			case "Check.Valid":
				flag = &r.verifyCert
			case "Check.Unexpired":
				flag = &r.verifyTime
			default:
				return rule{}, errors.New("unknown Check")
			}
			switch input[eq+1] {
			case '0':
				*flag = false
			case '1':
				*flag = true
			default:
				return rule{}, errors.New("Check value must be 0 or 1")
			}
			s = eq + 3
			continue
		}
		criteria, abbrev := &r.subject, term
		if dot := bytes.IndexByte(term, '.'); dot >= 0 {
			switch string(term[:dot]) {
			case "S", "Subject":
				criteria = &r.subject
			case "I", "Issuer":
				criteria = &r.issuer
			case "R", "Root":
				criteria = &r.root
			default:
				return rule{}, errors.New("unknown scope")
			}
			abbrev = term[dot+1:]
		}
		raw, end := de4514(input, eq+1)
		if end == eq+1 {
			return rule{}, errors.New("missing or unsupported value")
		}
		field, ok := fieldFromAbbrev(abbrev)
		if !ok {
			return rule{}, errors.New("unsupported field")
		}
		c := criterion{field: field, matchType: mt, raw: raw, utf8: latin1ToUTF8(raw)}
		dup := false
		for i := range *criteria {
			if (*criteria)[i].equal(&c) {
				dup = true
				break
			}
		}
		if !dup {
			*criteria = append(*criteria, c)
		}
		if end != len(input) && input[end] != ',' {
			return rule{}, errors.New("unexpected character after value")
		}
		s = end + 1
	}
	return r, nil
}

// parseVerifyPeers parses all
// verify-peers strings into alternative rules.
func parseVerifyPeers(verifyPeers [][]byte) ([]rule, error) {
	var rules []rule
	for _, vp := range verifyPeers {
		for _, expr := range splitRules(vp) {
			r, err := parseRule(expr)
			if err != nil {
				slog.Debug("FDBLibTLSVerifyPeersParseError", "config", string(vp), "reason", err.Error())
				return nil, fdberr.TLSError
			}
			rules = append(rules, r)
		}
	}
	return rules, nil
}

// splitRules splits a verify-peers string on | not preceded by \; it always
// yields at least one expression.
func splitRules(vp []byte) [][]byte {
	var rules [][]byte
	start, search := 0, 0
	for {
		i := bytes.IndexByte(vp[search:], '|')
		if i < 0 {
			break
		}
		split := search + i
		if split == start || vp[split-1] != '\\' {
			rules = append(rules, vp[start:split])
			start = split + 1
		}
		search = split + 1
	}
	return append(rules, vp[start:])
}

// de4514 unescapes an RFC 4514 value starting at start; it returns the
// value and the index just past it. Raw # values and leading spaces yield
// end == start; trailing unescaped spaces are dropped.
func de4514(input []byte, start int) ([]byte, int) {
	out := []byte{}
	if start < len(input) && (input[start] == '#' || input[start] == ' ') {
		return out, start
	}
	spaces := 0
	end := len(input)
	p := start
loop:
	for p < len(input) {
		switch c := input[p]; c {
		case '\\':
			if p+1 >= len(input) {
				end = p
				break loop
			}
			esc := input[p+1]
			switch esc {
			case ' ', '"', '#', '+', ',', ';', '<', '=', '>', '|', '\\':
				out = append(out, esc)
				p += 2
				spaces = 0
				continue
			}
			hi, ok1 := hexValue(esc)
			var lo byte
			ok2 := false
			if p+2 < len(input) {
				lo, ok2 = hexValue(input[p+2])
			}
			if !ok1 || !ok2 {
				end = p
				break loop
			}
			out = append(out, hi*16+lo)
			p += 3
			spaces = 0
		case '"', '+', ',', ';', '<', '>', 0:
			end = p
			break loop
		default:
			out = append(out, c)
			if c == ' ' {
				spaces++
			} else {
				spaces = 0
			}
			p++
		}
	}
	return out[:len(out)-spaces], end - spaces
}

// latin1ToUTF8 reads bytes as Latin-1 characters and encodes them as UTF-8.
func latin1ToUTF8(b []byte) string {
	ascii := true
	for _, c := range b {
		if c >= 0x80 {
			ascii = false
			break
		}
	}
	if ascii {
		return string(b)
	}
	r := make([]rune, len(b))
	for i, c := range b {
		r[i] = rune(c)
	}
	return string(r)
}

// asn1StringToUTF8 is ASN1_STRING_to_UTF8 for the string types OpenSSL
// converts; ok is false for other types or empty values.
func asn1StringToUTF8(tag int, data []byte) (string, bool) {
	var text string
	switch tag {
	case asn1.TagUTF8String:
		if !utf8.Valid(data) {
			return "", false
		}
		text = string(data)
	case asn1.TagNumericString, asn1.TagPrintableString, asn1.TagT61String, asn1.TagIA5String,
		asn1.TagUTCTime, asn1.TagGeneralizedTime, 26: // 26: VisibleString
		text = latin1ToUTF8(data)
	case asn1.TagBMPString: // UCS-2 big endian
		if len(data)%2 != 0 {
			return "", false
		}
		units := make([]uint16, len(data)/2)
		for i := range units {
			units[i] = uint16(data[2*i])<<8 | uint16(data[2*i+1])
		}
		for i := 0; i < len(units); i++ {
			// Reject unpaired surrogates.
			switch u := units[i]; {
			case utf16.IsSurrogate(rune(u)) && u < 0xDC00 && i+1 < len(units) && units[i+1] >= 0xDC00 && units[i+1] < 0xE000:
				i++
			case utf16.IsSurrogate(rune(u)):
				return "", false
			}
		}
		text = string(utf16.Decode(units))
	case 28: // UniversalString (UCS-4 big endian)
		if len(data)%4 != 0 {
			return "", false
		}
		r := make([]rune, len(data)/4)
		for i := range r {
			v := uint32(data[4*i])<<24 | uint32(data[4*i+1])<<16 | uint32(data[4*i+2])<<8 | uint32(data[4*i+3])
			if v > utf8.MaxRune || (v >= 0xD800 && v < 0xE000) {
				return "", false
			}
			r[i] = rune(v)
		}
		text = string(r)
	default:
		return "", false
	}
	return text, text != ""
}

// valueMatches is match_criteria_entry: empty values never match.
func valueMatches(mt matchType, crit, entry []byte) bool {
	if len(crit) == 0 || len(entry) == 0 {
		return false
	}
	switch mt {
	case matchPrefix:
		return bytes.HasPrefix(entry, crit)
	case matchSuffix:
		return bytes.HasSuffix(entry, crit)
	}
	return bytes.Equal(crit, entry)
}

// nameAttribute is one AttributeTypeAndValue of a distinguished name, with its raw string value.
type nameAttribute struct {
	Type  asn1.ObjectIdentifier
	Value asn1.RawValue
}

// nameAttributeSET is one RelativeDistinguishedName.
type nameAttributeSET []nameAttribute

// parseName flattens the attributes of a DER-encoded distinguished name.
func parseName(raw []byte) ([]nameAttribute, error) {
	var rdns []nameAttributeSET
	if rest, err := asn1.Unmarshal(raw, &rdns); err != nil || len(rest) != 0 {
		return nil, errors.New("malformed name")
	}
	var out []nameAttribute
	for _, rdn := range rdns {
		out = append(out, rdn...)
	}
	return out, nil
}

// checkCriteria checks that all criteria hold;
// the returned reason names the first failure ("" when all hold).
func checkCriteria(scope string, cert *x509.Certificate, rawName []byte, criteria []criterion) string {
	for i := range criteria {
		c := &criteria[i]
		var reason string
		if c.field.san {
			reason = matchSubjectAltName(cert, c)
		} else {
			reason = matchName(rawName, c)
		}
		if reason != "" {
			return scope + "." + reason + "[" + c.String() + "]"
		}
	}
	return ""
}

// matchName checks a name criterion: the attribute must occur exactly once and match.
func matchName(rawName []byte, c *criterion) string {
	attrs, err := parseName(rawName)
	if err != nil {
		return "Missing"
	}
	var entry *nameAttribute
	for i := range attrs {
		if attrs[i].Type.Equal(c.field.oid) {
			if entry != nil {
				return "MultipleNames"
			}
			entry = &attrs[i]
		}
	}
	if entry == nil {
		return "Missing"
	}
	if entry.Value.Class != asn1.ClassUniversal {
		return "NameMismatch"
	}
	value, ok := asn1StringToUTF8(entry.Value.Tag, entry.Value.Bytes)
	if !ok || !valueMatches(c.matchType, []byte(c.utf8), []byte(value)) {
		return "NameMismatch"
	}
	return ""
}

// oidSubjectAltName is the subject alternative name extension.
var oidSubjectAltName = asn1.ObjectIdentifier{2, 5, 29, 17}

// matchSubjectAltName checks a subjectAltName criterion: some typed SAN
// entry must match; unsupported entry types fail the criterion.
func matchSubjectAltName(cert *x509.Certificate, c *criterion) string {
	var ext []byte
	found := false
	for _, e := range cert.Extensions {
		if e.Id.Equal(oidSubjectAltName) {
			ext, found = e.Value, true
			break
		}
	}
	if !found {
		return "EmptySans"
	}
	var seq asn1.RawValue
	if rest, err := asn1.Unmarshal(ext, &seq); err != nil || len(rest) != 0 || !seq.IsCompound || seq.Tag != asn1.TagSequence {
		return "EmptySans"
	}
	for rest := seq.Bytes; len(rest) > 0; {
		var gn asn1.RawValue
		var err error
		if rest, err = asn1.Unmarshal(rest, &gn); err != nil {
			return "EmptySans"
		}
		if gn.Class != asn1.ClassContextSpecific {
			continue
		}
		var prefix string
		entry := gn.Bytes
		switch gn.Tag {
		case 0:
			return "GEN_OTHERNAME"
		case 1:
			prefix = "EMAIL:"
		case 2:
			prefix = "DNS:"
		case 3:
			return "GEN_X400"
		case 4:
			return "GEN_DIRNAME"
		case 5:
			return "GEN_EDIPARTY"
		case 6:
			prefix = "URI:"
		case 7:
			addr, ok := netip.AddrFromSlice(gn.Bytes)
			if !ok {
				continue
			}
			prefix, entry = "IP:", []byte(addr.String())
		case 8:
			return "GEN_RID"
		default:
			continue
		}
		if value, ok := bytes.CutPrefix(c.raw, []byte(prefix)); ok && valueMatches(c.matchType, value, entry) {
			return ""
		}
	}
	return "NoMatchingEntry"
}

// check applies the rule's name criteria to the leaf and the trust anchor.
func (r *rule) check(leaf, root *x509.Certificate) string {
	if reason := checkCriteria("Subject", leaf, leaf.RawSubject, r.subject); reason != "" {
		return reason
	}
	if reason := checkCriteria("Issuer", leaf, leaf.RawIssuer, r.issuer); reason != "" {
		return reason
	}
	if len(r.root) > 0 {
		return checkCriteria("RootCert.Subject", root, root.RawSubject, r.root)
	}
	return ""
}

// peerVerifier enforces the verify-peers rules, without hostname checks.
type peerVerifier struct {
	// roots are the configured CA certificates; pool holds the same certificates.
	roots []*x509.Certificate
	pool  *x509.CertPool
	// rules are the alternatives; any one passing accepts the peer.
	rules []rule
}

// errUnknownIssuer reports a chain that ends in no configured CA.
var errUnknownIssuer = errors.New("tls: certificate signed by unknown authority")

// verifyChain validates the presented chain at time at, returning the index
// of the CA certificate it ends in.
func (v *peerVerifier) verifyChain(leaf *x509.Certificate, inters []*x509.Certificate, at time.Time) (int, error) {
	pool := x509.NewCertPool()
	for _, c := range inters {
		pool.AddCert(c)
	}
	chains, err := leaf.Verify(x509.VerifyOptions{
		Roots:         v.pool,
		Intermediates: pool,
		CurrentTime:   at,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err == nil {
		chain := chains[0]
		if i := v.rootIndex(chain[len(chain)-1]); i >= 0 {
			return i, nil
		}
		return 0, errUnknownIssuer
	}
	return 0, err
}

// verifyChainIgnoringTime validates every non-time constraint in the presented
// chain (Check.Unexpired=0), returning the CA index it ends in.
func (v *peerVerifier) verifyChainIgnoringTime(leaf *x509.Certificate, inters []*x509.Certificate) (int, error) {
	// Go's verifier has no option to skip validity checks. Give private copies
	// of the parsed metadata a common validity interval instead. Raw DER and
	// signed contents stay untouched, so signatures and all other constraints
	// are still checked. Explicit, non-system pools keep verification in Go,
	// where these parsed validity fields are used rather than reparsed DER.
	at := time.Unix(0, 0)
	withoutTime := func(c *x509.Certificate) *x509.Certificate {
		copy := *c
		copy.NotBefore = at.Add(-time.Hour)
		copy.NotAfter = at.Add(time.Hour)
		return &copy
	}
	untimed := peerVerifier{roots: v.roots, pool: x509.NewCertPool()}
	for _, root := range v.roots {
		untimed.pool.AddCert(withoutTime(root))
	}
	clonedInters := make([]*x509.Certificate, len(inters))
	for i, c := range inters {
		clonedInters[i] = withoutTime(c)
	}
	return untimed.verifyChain(withoutTime(leaf), clonedInters, at)
}

func (v *peerVerifier) rootIndex(c *x509.Certificate) int {
	for i, r := range v.roots {
		if bytes.Equal(r.Raw, c.Raw) {
			return i
		}
	}
	return -1
}

// errPolicy reports a server certificate rejected by every verify-peers rule.
var errPolicy = errors.New("tls: server certificate rejected by verify-peers policy")

// verify is the tls.Config.VerifyPeerCertificate hook.
func (v *peerVerifier) verify(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return v.verifyAt(rawCerts, time.Now())
}

func (v *peerVerifier) verifyAt(rawCerts [][]byte, now time.Time) error {
	for i := range v.rules {
		if !v.rules[i].verifyCert {
			return nil
		}
	}
	if len(rawCerts) == 0 {
		return errors.New("tls: server presented no certificate")
	}
	certs := make([]*x509.Certificate, len(rawCerts))
	for i, raw := range rawCerts {
		c, err := x509.ParseCertificate(raw)
		if err != nil {
			return err
		}
		certs[i] = c
	}
	leaf, inters := certs[0], certs[1:]
	timedIdx, timedErr := v.verifyChain(leaf, inters, now)
	untimedDone := false
	var untimedIdx int
	var untimedErr, chainErr error
	ruleFailed := false
	for i := range v.rules {
		r := &v.rules[i]
		idx, err := timedIdx, timedErr
		// Expired intermediates can be reported as unknown-authority errors.
		// Recheck every failed chain for this rule, preserving non-time checks.
		if err != nil && !r.verifyTime {
			if !untimedDone {
				untimedIdx, untimedErr = v.verifyChainIgnoringTime(leaf, inters)
				untimedDone = true
			}
			idx, err = untimedIdx, untimedErr
		}
		if err != nil {
			slog.Debug("TLSPolicyFailure: chain verification failed", "rule", i, "reason", err.Error())
			if chainErr == nil {
				chainErr = err
			}
			continue
		}
		reason := r.check(leaf, v.roots[idx])
		if reason == "" {
			return nil
		}
		slog.Debug("TLSPolicyFailure", "rule", i, "reason", reason)
		ruleFailed = true
	}
	if chainErr != nil && !ruleFailed {
		return chainErr
	}
	return errPolicy
}
