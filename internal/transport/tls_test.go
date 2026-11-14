package transport

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/binary"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/options"
	"github.com/can1357/fdbgo/internal/wire"
)

// nameAttr is one distinguished-name attribute with an explicit string tag.
type nameAttr struct {
	oid   asn1.ObjectIdentifier
	tag   int
	value []byte
}

var (
	oidC   = asn1.ObjectIdentifier{2, 5, 4, 6}
	oidCN  = asn1.ObjectIdentifier{2, 5, 4, 3}
	oidL   = asn1.ObjectIdentifier{2, 5, 4, 7}
	oidO   = asn1.ObjectIdentifier{2, 5, 4, 10}
	oidOU  = asn1.ObjectIdentifier{2, 5, 4, 11}
	oidUID = asn1.ObjectIdentifier{0, 9, 2342, 19200300, 100, 1, 1}
	oidDC  = asn1.ObjectIdentifier{0, 9, 2342, 19200300, 100, 1, 25}
)

func utf8Attr(oid asn1.ObjectIdentifier, v string) nameAttr {
	return nameAttr{oid, asn1.TagUTF8String, []byte(v)}
}

// setAttr replaces the attribute of the same type in place, or appends it.
func setAttr(name []nameAttr, a nameAttr) []nameAttr {
	for i := range name {
		if name[i].oid.Equal(a.oid) {
			name = slices.Clone(name)
			name[i] = a
			return name
		}
	}
	return append(slices.Clone(name), a)
}

// rawName DER-encodes a name with one attribute per RDN.
func rawName(t testing.TB, name []nameAttr) []byte {
	t.Helper()
	var rdns []nameAttributeSET
	for _, a := range name {
		rdns = append(rdns, nameAttributeSET{{Type: a.oid, Value: asn1.RawValue{Class: asn1.ClassUniversal, Tag: a.tag, Bytes: a.value}}})
	}
	der, err := asn1.Marshal(rdns)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

// testCA is a CA certificate and its signing key.
type testCA struct {
	cert *x509.Certificate
	key  crypto.Signer
}

func (ca *testCA) pem() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})
}

func newKey(t testing.TB) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

var serial int64

func create(t testing.TB, tmpl *x509.Certificate, key crypto.Signer, parent *testCA) *x509.Certificate {
	t.Helper()
	serial++
	tmpl.SerialNumber = big.NewInt(serial)
	if tmpl.NotBefore.IsZero() {
		tmpl.NotBefore = time.Now().Add(-time.Hour)
		tmpl.NotAfter = time.Now().Add(24 * time.Hour)
	}
	parentCert, parentKey := tmpl, key
	if parent != nil {
		parentCert, parentKey = parent.cert, parent.key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parentCert, key.Public(), parentKey)
	if err != nil {
		t.Fatal(err)
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func caTemplate(t testing.TB, cn string) *x509.Certificate {
	return &x509.Certificate{
		RawSubject:            rawName(t, []nameAttr{utf8Attr(oidO, "FDB Test"), utf8Attr(oidCN, cn)}),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
}

// rootCA returns a self-signed root with the given common name.
func rootCA(t testing.TB, cn string) *testCA {
	key := newKey(t)
	return &testCA{cert: create(t, caTemplate(t, cn), key, nil), key: key}
}

// intermediate returns a CA signed by ca.
func (ca *testCA) intermediate(t testing.TB, cn string) *testCA {
	key := newKey(t)
	return &testCA{cert: create(t, caTemplate(t, cn), key, ca), key: key}
}

// leafName resembles an FDB server certificate's subject.
func leafName() []nameAttr {
	return []nameAttr{
		{oidC, asn1.TagPrintableString, []byte("US")},
		utf8Attr(oidO, "FDB Test"),
		utf8Attr(oidOU, "FDB Team"),
		utf8Attr(oidCN, "fdb-server"),
		utf8Attr(oidUID, "user-1"),
		{oidDC, asn1.TagIA5String, []byte("fdb")},
	}
}

// leafTemplate is a server/client certificate template with the given subject.
func leafTemplate(t testing.TB, name []nameAttr) *x509.Certificate {
	uri, _ := url.Parse("fdb://cluster/server")
	return &x509.Certificate{
		RawSubject:     rawName(t, name),
		DNSNames:       []string{"server.fdb.test"},
		IPAddresses:    []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		URIs:           []*url.URL{uri},
		EmailAddresses: []string{"ops@fdb.test"},
		ExtKeyUsage:    []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		KeyUsage:       x509.KeyUsageDigitalSignature,
	}
}

// sign issues a leaf from tmpl.
func (ca *testCA) sign(t testing.TB, tmpl *x509.Certificate) (*x509.Certificate, *ecdsa.PrivateKey) {
	key := newKey(t)
	return create(t, tmpl, key, ca), key
}

func certPEM(c *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw})
}

func keyPEM(t testing.TB, k crypto.Signer) []byte {
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

// verifier trusts caPEM with the given verify-peers string.
func verifier(t testing.TB, caPEM []byte, verifyPeers string) *peerVerifier {
	t.Helper()
	var c tlsConfig
	c.setCABytes(caPEM)
	c.setVerifyPeers([]byte(verifyPeers))
	v, err := c.peerVerifier()
	if err != nil {
		t.Fatalf("%q: %v", verifyPeers, err)
	}
	return v
}

func accepts(v *peerVerifier, leaf *x509.Certificate, chain ...*x509.Certificate) bool {
	raw := [][]byte{leaf.Raw}
	for _, c := range chain {
		raw = append(raw, c.Raw)
	}
	return v.verify(raw, nil) == nil
}

func envOf(vars map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	}
}

func criteriaStrings(cs []criterion) []string {
	var out []string
	for i := range cs {
		out = append(out, cs[i].String())
	}
	return out
}

func TestSplitsAlternatives(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{`a|b\|c|`, []string{"a", `b\|c`, ""}},
		{"", []string{""}},
		{"|x", []string{"", "x"}},
	} {
		var got []string
		for _, r := range splitRules([]byte(tc.in)) {
			got = append(got, string(r))
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("%q: got %q want %q", tc.in, got, tc.want)
		}
	}
}

func TestParsesDocumentedExamples(t *testing.T) {
	r, err := parseRule([]byte("Check.Unexpired=0,I.C=US,C=US,S.O=Apple Inc."))
	if err != nil {
		t.Fatal(err)
	}
	if !r.verifyCert || r.verifyTime {
		t.Fatal("checks")
	}
	if got := criteriaStrings(r.issuer); !slices.Equal(got, []string{"C=US"}) {
		t.Fatal(got)
	}
	if got := criteriaStrings(r.subject); !slices.Equal(got, []string{"C=US", "O=Apple Inc."}) {
		t.Fatal(got)
	}
	r, err = parseRule([]byte("S.OU>=FDB,S.OU<=Team,S.subjectAltName=DNS:test.foundationdb.org"))
	if err != nil {
		t.Fatal(err)
	}
	if got := criteriaStrings(r.subject); !slices.Equal(got, []string{"OU>=FDB", "OU<=Team", "subjectAltName=DNS:test.foundationdb.org"}) {
		t.Fatal(got)
	}
	r, err = parseRule([]byte("Root.CN=r,Issuer.O=i,Subject.L=s,R.UID=u,DC=d,ST=x"))
	if err != nil || len(r.root) != 2 || len(r.issuer) != 1 || len(r.subject) != 3 {
		t.Fatal(r, err)
	}
	if r, _ := parseRule([]byte("Check.Valid=0")); r.verifyCert {
		t.Fatal("Check.Valid=0")
	}
	empty, _ := parseRule(nil)
	def, _ := parseRule([]byte("Check.Valid=1"))
	if empty.verifyCert != def.verifyCert || empty.verifyTime != def.verifyTime || len(empty.subject)+len(def.subject) != 0 {
		t.Fatal("empty rule differs from Check.Valid=1")
	}
	if r, _ := parseRule([]byte("CN=a,CN=a")); len(r.subject) != 1 {
		t.Fatal("duplicate criteria kept")
	}
}

func TestUnescapesRFC4514Values(t *testing.T) {
	r, err := parseRule([]byte(`CN=a\,b\2Cc\ ,O=x\|y=z,L=\41\62`))
	if err != nil {
		t.Fatal(err)
	}
	var raw []string
	for _, c := range r.subject {
		raw = append(raw, string(c.raw))
	}
	if !slices.Equal(raw, []string{"a,b,c ", "x|y=z", "Ab"}) {
		t.Fatalf("%q", raw)
	}
	r, err = parseRule([]byte(`CN=\E9t\E9`))
	if err != nil || r.subject[0].utf8 != "été" {
		t.Fatal(r, err)
	}
}

func TestRejectsMalformedRules(t *testing.T) {
	for _, bad := range []string{
		"CN", "X=1", "Check.Valid=2", "Check.Valid=10", "Check.Valid>=1", "Check.Foo=1", "Check.Valid=",
		"Q.CN=a", "CN=#0403", "CN= a", "CN=", "CN=a;b", "CN=a ,O=b", "OU=trail  ", `CN=a\zz`,
		"issuerAltName=DNS:x",
	} {
		if _, err := parseRule([]byte(bad)); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if _, err := parseVerifyPeers([][]byte{[]byte("CN=a|X=1")}); err != fdberr.TLSError {
		t.Fatalf("got %v", err)
	}
	if rules, err := parseVerifyPeers([][]byte{[]byte("CN=a|CN=b"), []byte("O=c")}); err != nil || len(rules) != 3 {
		t.Fatal(rules, err)
	}
}

func TestVerifiesChainAndSubjectFields(t *testing.T) {
	ca := rootCA(t, "FDB Test Root")
	leaf, _ := ca.sign(t, leafTemplate(t, leafName()))
	for _, ok := range []string{
		"Check.Valid=1",
		"",
		"S.CN=fdb-server",
		"CN>=fdb-",
		"CN<=-server",
		"C=US,O=FDB Test,OU=FDB Team",
		"UID=user-1,DC=fdb",
		"I.CN=FDB Test Root,I.O=FDB Test",
		"R.CN=FDB Test Root",
		"S.CN=nope|S.CN=fdb-server",
		"S.subjectAltName=DNS:server.fdb.test",
		"S.subjectAltName>=DNS:server.,S.subjectAltName<=DNS:.fdb.test",
		"subjectAltName=IP:127.0.0.1",
		"subjectAltName=IP:::1",
		"subjectAltName=EMAIL:ops@fdb.test",
		"subjectAltName=URI:fdb://cluster/server",
		"I.subjectAltName=DNS:server.fdb.test",
	} {
		if !accepts(verifier(t, ca.pem(), ok), leaf) {
			t.Errorf("%q rejected", ok)
		}
	}
	for _, rejected := range []string{
		"S.CN=fdb-client",
		"CN<=-client",
		"CN>=server",
		"L=Cupertino",
		"I.CN=fdb-server",
		"R.CN=Other Root",
		"S.CN=nope|S.CN=nada",
		"subjectAltName=DNS:other.fdb.test",
		"subjectAltName=server.fdb.test",
		"subjectAltName=IP:10.0.0.1",
		"R.subjectAltName=DNS:server.fdb.test",
	} {
		if accepts(verifier(t, ca.pem(), rejected), leaf) {
			t.Errorf("%q accepted", rejected)
		}
	}
}

func TestVerifiesRootOfIntermediateChain(t *testing.T) {
	root := rootCA(t, "FDB Test Root")
	inter := root.intermediate(t, "FDB Intermediate")
	leaf, _ := inter.sign(t, leafTemplate(t, leafName()))
	if !accepts(verifier(t, root.pem(), "R.CN=FDB Test Root,I.CN=FDB Intermediate"), leaf, inter.cert) {
		t.Error("chain rejected")
	}
	if accepts(verifier(t, root.pem(), "R.CN=FDB Intermediate"), leaf, inter.cert) {
		t.Error("intermediate treated as root")
	}
	if accepts(verifier(t, root.pem(), "Check.Valid=1"), leaf) {
		t.Error("chain without its intermediate accepted")
	}
}

func TestUntrustedChainNeedsCheckValid0(t *testing.T) {
	trusted := rootCA(t, "Trusted Root")
	other := rootCA(t, "Other Root")
	leaf, _ := other.sign(t, leafTemplate(t, leafName()))
	for vp, want := range map[string]bool{
		"Check.Valid=1":           false,
		"Check.Unexpired=0":       false,
		"Check.Valid=0":           true,
		"S.CN=nope|Check.Valid=0": true,
	} {
		if got := accepts(verifier(t, trusted.pem(), vp), leaf); got != want {
			t.Errorf("%q: accepted=%v", vp, got)
		}
	}
	var noCA tlsConfig
	noCA.setVerifyPeers([]byte("Check.Valid=1"))
	v, err := noCA.peerVerifier()
	if err != nil {
		t.Fatal(err)
	}
	if accepts(v, leaf) {
		t.Error("accepted without any CA")
	}
}

func TestExpiryRespectsCheckUnexpired(t *testing.T) {
	now := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, scenario := range []string{"leaf", "intermediate", "root", "disjoint", "not-yet-valid"} {
		t.Run(scenario, func(t *testing.T) {
			templates := []*x509.Certificate{
				caTemplate(t, "FDB Test Root"),
				caTemplate(t, "FDB Intermediate"),
				leafTemplate(t, leafName()),
			}
			for i, tmpl := range templates {
				tmpl.NotBefore, tmpl.NotAfter = now.Add(-time.Hour), now.Add(time.Hour)
				if scenario == []string{"root", "intermediate", "leaf"}[i] || scenario == "disjoint" {
					tmpl.NotBefore = time.Date(2000+2*i, 1, 1, 0, 0, 0, 0, time.UTC)
					tmpl.NotAfter = tmpl.NotBefore.Add(time.Hour)
				}
				if scenario == "not-yet-valid" {
					tmpl.NotBefore, tmpl.NotAfter = now.Add(time.Hour), now.Add(2*time.Hour)
				}
			}
			rootKey, interKey := newKey(t), newKey(t)
			root := &testCA{cert: create(t, templates[0], rootKey, nil), key: rootKey}
			inter := &testCA{cert: create(t, templates[1], interKey, root), key: interKey}
			leaf, _ := inter.sign(t, templates[2])
			for vp, want := range map[string]bool{
				"Check.Valid=1":                           false,
				"Check.Unexpired=0":                       true,
				"Check.Unexpired=0,CN=fdb-server":         true,
				"Check.Unexpired=0,CN=nope":               false,
				"CN=nope|Check.Unexpired=0,CN=fdb-server": true,
			} {
				v := verifier(t, root.pem(), vp)
				if got := v.verifyAt([][]byte{leaf.Raw, inter.cert.Raw}, now) == nil; got != want {
					t.Errorf("%q: accepted=%v", vp, got)
				}
				if !v.roots[0].NotBefore.Equal(root.cert.NotBefore) || !v.roots[0].NotAfter.Equal(root.cert.NotAfter) {
					t.Error("verification modified shared root validity")
				}
			}
		})
	}
}

func TestCheckUnexpiredPreservesChainValidation(t *testing.T) {
	now := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		target int
		change func(*x509.Certificate)
	}{
		{"root-name-constraints", 0, func(c *x509.Certificate) {
			c.PermittedDNSDomainsCritical = true
			c.PermittedDNSDomains = []string{"other.test"}
		}},
		{"intermediate-name-constraints", 1, func(c *x509.Certificate) {
			c.PermittedDNSDomainsCritical = true
			c.PermittedDNSDomains = []string{"other.test"}
		}},
		{"intermediate-eku", 1, func(c *x509.Certificate) {
			c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}},
		{"leaf-eku", 3, func(c *x509.Certificate) {
			c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}},
		{"root-path-length", 0, func(c *x509.Certificate) {
			c.MaxPathLen, c.MaxPathLenZero = 0, true
		}},
		{"intermediate-path-length", 1, func(c *x509.Certificate) {
			c.MaxPathLen, c.MaxPathLenZero = 0, true
		}},
		{"root-critical-extension", 0, nil},
		{"intermediate-critical-extension", 1, nil},
		{"leaf-critical-extension", 3, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			templates := []*x509.Certificate{
				caTemplate(t, "FDB Test Root"),
				caTemplate(t, "FDB Intermediate"),
				caTemplate(t, "FDB Nested Intermediate"),
				leafTemplate(t, leafName()),
			}
			for _, tmpl := range templates {
				tmpl.NotBefore, tmpl.NotAfter = now.Add(-time.Hour), now.Add(time.Hour)
			}
			templates[3].NotBefore, templates[3].NotAfter = now.Add(-3*time.Hour), now.Add(-2*time.Hour)
			if tc.change != nil {
				tc.change(templates[tc.target])
			} else {
				templates[tc.target].ExtraExtensions = []pkix.Extension{{
					Id: asn1.ObjectIdentifier{1, 2, 3, 4}, Critical: true, Value: []byte{5, 0},
				}}
			}
			certs := make([]*x509.Certificate, len(templates))
			var parent *testCA
			for i, tmpl := range templates {
				key := newKey(t)
				certs[i] = create(t, tmpl, key, parent)
				parent = &testCA{cert: certs[i], key: key}
			}
			raw := [][]byte{certs[3].Raw, certs[2].Raw, certs[1].Raw}
			for _, vp := range []string{
				"Check.Unexpired=0",
				"CN=nope|Check.Unexpired=0,CN=fdb-server",
			} {
				if err := verifier(t, certPEM(certs[0]), vp).verifyAt(raw, now); err == nil {
					t.Errorf("%q accepted invalid expired chain", vp)
				}
			}
		})
	}
}

func TestDirectlyTrustedLeafRequiresFullVerification(t *testing.T) {
	now := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, scenario := range []string{"server-auth", "client-auth-only", "critical-extension"} {
		t.Run(scenario, func(t *testing.T) {
			for _, expired := range []bool{false, true} {
				tmpl := leafTemplate(t, leafName())
				tmpl.NotBefore, tmpl.NotAfter = now.Add(-time.Hour), now.Add(time.Hour)
				if expired {
					tmpl.NotBefore, tmpl.NotAfter = now.Add(-3*time.Hour), now.Add(-2*time.Hour)
				}
				switch scenario {
				case "client-auth-only":
					tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
				case "critical-extension":
					tmpl.ExtraExtensions = []pkix.Extension{{
						Id: asn1.ObjectIdentifier{1, 2, 3, 4}, Critical: true, Value: []byte{5, 0},
					}}
				}
				leaf := create(t, tmpl, newKey(t), nil)
				for _, vp := range []string{"Check.Valid=1", "Check.Unexpired=0"} {
					want := scenario == "server-auth" && (!expired || vp == "Check.Unexpired=0")
					got := verifier(t, certPEM(leaf), vp).verifyAt([][]byte{leaf.Raw}, now) == nil
					if got != want {
						t.Errorf("expired=%v, %q: accepted=%v, want %v", expired, vp, got, want)
					}
				}
			}
		})
	}
}

func TestTrustsCACertificatePresentedAsLeaf(t *testing.T) {
	ca := rootCA(t, "fdb-self-signed")
	if !accepts(verifier(t, ca.pem(), "CN=fdb-self-signed,R.CN=fdb-self-signed"), ca.cert) {
		t.Error("self-signed CA leaf rejected")
	}
	other := rootCA(t, "fdb-self-signed")
	if accepts(verifier(t, other.pem(), "Check.Valid=1"), ca.cert) {
		t.Error("lookalike CA accepted")
	}
}

func TestMatchesNonASCIINamesLikeOpenSSL(t *testing.T) {
	ca := rootCA(t, "FDB Test Root")
	var bmp []byte
	for _, u := range utf16.Encode([]rune("Grüße")) {
		bmp = binary.BigEndian.AppendUint16(bmp, u)
	}
	name := setAttr(leafName(), nameAttr{oidCN, asn1.TagBMPString, bmp})
	name = setAttr(name, utf8Attr(oidL, "Zürich"))
	leaf, _ := ca.sign(t, leafTemplate(t, name))
	if !accepts(verifier(t, ca.pem(), `CN=Gr\FC\DFe,L=Z\FCrich`), leaf) {
		t.Error("escaped Latin-1 criteria rejected")
	}
	// libfdb_c reads the criterion as Latin-1, so raw UTF-8 bytes in the expression do not match.
	if accepts(verifier(t, ca.pem(), "L=Zürich"), leaf) {
		t.Error("raw UTF-8 criterion accepted")
	}
}

func TestBuildsFromBytes(t *testing.T) {
	ca := rootCA(t, "FDB Test Root")
	leaf, key := ca.sign(t, leafTemplate(t, leafName()))
	var c tlsConfig
	c.setCertBytes(certPEM(leaf))
	c.setKeyBytes(keyPEM(t, key))
	c.setCABytes(ca.pem())
	c.setVerifyPeers([]byte("S.CN=fdb-server"))
	if _, err := c.build(); err != nil {
		t.Fatal(err)
	}
	for name, breakIt := range map[string]func(*tlsConfig){
		"bad verify peers": func(c *tlsConfig) { c.setVerifyPeers([]byte("S.XX=1")) },
		"bad CA":           func(c *tlsConfig) { c.setCABytes([]byte("not a certificate")) },
		"mismatched key":   func(c *tlsConfig) { c.setKeyBytes(keyPEM(t, newKey(t))) },
		"empty key":        func(c *tlsConfig) { c.setKeyBytes(nil) },
	} {
		broken := c
		breakIt(&broken)
		if _, err := broken.build(); err != fdberr.TLSError {
			t.Errorf("%s: got %v", name, err)
		}
	}
	var noClientCert tlsConfig
	noClientCert.setCABytes(ca.pem())
	if _, err := noClientCert.build(); err != nil {
		t.Fatal(err)
	}
	var empty tlsConfig
	if _, err := empty.build(); err != nil {
		t.Fatal(err)
	}
}

func readTestdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func publicDER(t *testing.T, k crypto.Signer) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(k.Public())
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func TestDecryptsPasswordProtectedKeys(t *testing.T) {
	// Fixtures made by openssl: an EC key (SEC1), the same key encrypted by
	// `openssl ec -aes128` and `-des3` with password fdb-legacy, and as PKCS#8
	// by `openssl pkcs8 -topk8 -v2 aes-256-cbc` (PBKDF2-HMAC-SHA256) and
	// `-v2 des3 -v2prf hmacWithSHA1` with password fdb-pkcs8.
	plain, err := parsePrivateKey(readTestdata(t, "legacy_plain.pem"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := plain.(*ecdsa.PrivateKey); !ok {
		t.Fatalf("plain key is %T", plain)
	}
	want := publicDER(t, plain)
	for file, password := range map[string]string{
		"legacy_aes.pem":      "fdb-legacy",
		"legacy_des3.pem":     "fdb-legacy",
		"pkcs8_aes256.pem":    "fdb-pkcs8",
		"pkcs8_des3_sha1.pem": "fdb-pkcs8",
	} {
		pemBytes := readTestdata(t, file)
		k, err := parsePrivateKey(pemBytes, []byte(password))
		if err != nil {
			t.Errorf("%s: %v", file, err)
			continue
		}
		if !bytes.Equal(publicDER(t, k), want) {
			t.Errorf("%s: wrong key", file)
		}
		if _, err := parsePrivateKey(pemBytes, []byte("wrong")); err != fdberr.TLSError {
			t.Errorf("%s with wrong password: got %v", file, err)
		}
	}
	if _, err := parsePrivateKey([]byte("no key here"), nil); err != fdberr.TLSError {
		t.Fatalf("got %v", err)
	}
}

func TestDecryptsEveryLegacyCipher(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	want := publicDER(t, rsaKey)
	for _, alg := range []x509.PEMCipher{x509.PEMCipherDES, x509.PEMCipher3DES, x509.PEMCipherAES128, x509.PEMCipherAES192, x509.PEMCipherAES256} {
		//lint:ignore SA1019 legacy PEM encryption is exactly what is under test.
		blk, err := x509.EncryptPEMBlock(rand.Reader, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(rsaKey), []byte("pw"), alg)
		if err != nil {
			t.Fatal(err)
		}
		k, err := parsePrivateKey(pem.EncodeToMemory(blk), []byte("pw"))
		if err != nil {
			t.Errorf("cipher %d: %v", alg, err)
			continue
		}
		if !bytes.Equal(publicDER(t, k), want) {
			t.Errorf("cipher %d: wrong key", alg)
		}
	}
}

func TestPasswordFromOptionOrEnv(t *testing.T) {
	c := tlsConfigFromEnvWith(envOf(map[string]string{envPassword: "fdb-pkcs8"}), "")
	c.setKeyBytes(readTestdata(t, "pkcs8_aes256.pem"))
	if string(c.effectivePassword()) != "fdb-pkcs8" {
		t.Fatal("env password")
	}
	c.setPassword([]byte("explicit"))
	if string(c.effectivePassword()) != "explicit" {
		t.Fatal("explicit password")
	}
	c.setPassword(nil)
	if string(c.effectivePassword()) != "fdb-pkcs8" {
		t.Fatal("cleared password")
	}
	var d tlsConfig
	if len(d.effectivePassword()) != 0 {
		t.Fatal("default password")
	}
}

func TestFilePrecedenceMatchesFlow(t *testing.T) {
	dir, defaults := t.TempDir(), t.TempDir()
	write := func(dir, name, contents string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	envCert := write(dir, "env.pem", "env")
	explicitCert := write(dir, "explicit.pem", "explicit")
	cert := func(c *tlsConfig) string {
		b, err := c.certificatePEM()
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			return "<none>"
		}
		return string(b)
	}
	expect := func(got, want string) {
		t.Helper()
		if got != want {
			t.Fatalf("got %q want %q", got, want)
		}
	}

	c := tlsConfigFromEnvWith(envOf(map[string]string{envCertificateFile: envCert}), defaults)
	c.setCertBytes([]byte("bytes"))
	expect(cert(&c), "env")
	c.setCertPath(explicitCert)
	expect(cert(&c), "explicit")
	c.setCertPath("")
	expect(cert(&c), "env")

	c = tlsConfigFromEnvWith(envOf(nil), defaults)
	if c.isConfigured() {
		t.Fatal("configured without material")
	}
	c.setCertBytes([]byte("bytes"))
	if !c.isConfigured() {
		t.Fatal("bytes not configured")
	}
	expect(cert(&c), "bytes")
	if fresh := tlsConfigFromEnvWith(envOf(nil), defaults); fresh.isConfigured() {
		t.Fatal("configured without default files")
	}
	write(defaults, defaultCertFileName, "default")
	if fresh := tlsConfigFromEnvWith(envOf(nil), defaults); !fresh.isConfigured() {
		t.Fatal("default cert.pem not seen")
	}
	write(defaults, defaultKeyFileName, "default key")
	expect(cert(&c), "default")
	if k, _ := c.keyPEM(); string(k) != "default key" {
		t.Fatalf("key %q", k)
	}
	if ca, _ := c.caPEM(); ca != nil {
		t.Fatal("CA has no default file")
	}

	c = tlsConfigFromEnvWith(envOf(map[string]string{envCertificateFile: ""}), defaults)
	expect(cert(&c), "<none>")
	c.setCertBytes([]byte("bytes"))
	expect(cert(&c), "bytes")

	c = tlsConfigFromEnvWith(envOf(map[string]string{envCAFile: envCert}), "")
	if !c.isConfigured() {
		t.Fatal("env CA not configured")
	}
	if ca, _ := c.caPEM(); string(ca) != "env" {
		t.Fatalf("CA %q", ca)
	}
	if e := tlsConfigFromEnvWith(envOf(map[string]string{envCAFile: ""}), ""); e.isConfigured() {
		t.Fatal("empty env CA configured")
	}

	var missing tlsConfig
	missing.setCAPath(filepath.Join(dir, "missing.pem"))
	if _, err := missing.caPEM(); err != fdberr.FileNotFound {
		t.Fatalf("got %v", err)
	}
	if _, err := missing.build(); err != fdberr.FileNotFound {
		t.Fatalf("got %v", err)
	}
	big := tlsConfig{}
	big.setCAPath(write(dir, "big.pem", strings.Repeat("x", certFileMaxSize+1)))
	if _, err := big.caPEM(); err != fdberr.FileTooLarge {
		t.Fatalf("got %v", err)
	}
}

func TestVerifyPeersAndPlaintextPrecedence(t *testing.T) {
	vp := func(c *tlsConfig) []string {
		var out []string
		for _, v := range c.effectiveVerifyPeers() {
			out = append(out, string(v))
		}
		return out
	}
	var d tlsConfig
	if got := vp(&d); !slices.Equal(got, []string{defaultVerifyPeers}) {
		t.Fatal(got)
	}
	c := tlsConfigFromEnvWith(envOf(map[string]string{envVerifyPeers: "CN=env", envDisablePlaintextConnection: "1"}), "")
	if got := vp(&c); !slices.Equal(got, []string{"CN=env"}) {
		t.Fatal(got)
	}
	if !c.disablePlaintext() {
		t.Fatal("env 1")
	}
	c.setVerifyPeers([]byte("CN=a"))
	c.setVerifyPeers([]byte("CN=b"))
	if got := vp(&c); !slices.Equal(got, []string{"CN=b"}) {
		t.Fatal(got)
	}
	c = tlsConfigFromEnvWith(envOf(map[string]string{envDisablePlaintextConnection: "0"}), "")
	c.setDisablePlaintext(true)
	if c.disablePlaintext() {
		t.Fatal("env 0 must win")
	}
	c = tlsConfigFromEnvWith(envOf(map[string]string{envDisablePlaintextConnection: "yes"}), "")
	if c.disablePlaintext() {
		t.Fatal("invalid env")
	}
	c.setDisablePlaintext(true)
	if !c.disablePlaintext() {
		t.Fatal("option ignored")
	}
}

func TestAppliesNetworkOptions(t *testing.T) {
	var c tlsConfig
	for _, o := range []struct {
		code  int
		param string
	}{
		{options.NetworkTLSCertBytes, "cert"},
		{options.NetworkTLSKeyPath, "/k.pem"},
		{options.NetworkTLSCaBytes, "ca"},
		{options.NetworkTLSPassword, "pw"},
		{options.NetworkTLSVerifyPeers, "CN=a"},
		{options.NetworkTLSVerifyPeers, "CN=b"},
		{options.NetworkTLSPlugin, "ignored"},
		{options.NetworkTLSDisablePlaintextConnection, ""},
	} {
		if !c.applyNetworkOption(o.code, []byte(o.param)) {
			t.Fatalf("option %d not applied", o.code)
		}
	}
	if c.applyNetworkOption(options.NetworkBuggifyEnable, nil) {
		t.Fatal("buggify applied as TLS option")
	}
	if c.cert == nil || c.cert.isPath || string(c.cert.bytes) != "cert" ||
		c.key == nil || !c.key.isPath || c.key.path != "/k.pem" ||
		c.ca == nil || c.ca.isPath || string(c.ca.bytes) != "ca" {
		t.Fatal(c.String())
	}
	if string(c.effectivePassword()) != "pw" || len(c.verifyPeers) != 1 || string(c.verifyPeers[0]) != "CN=b" || !c.disablePlaintext() {
		t.Fatal(c.String())
	}
	if strings.Contains(c.String(), "pw") || strings.Contains(c.String(), "cert)") {
		t.Fatalf("String leaks secrets: %s", c.String())
	}
}

// serveOnce accepts one TLS connection requiring a client certificate from
// clientCA, reads 5 bytes and answers "world".
func serveOnce(ln net.Listener, cert tls.Certificate, clientCA *x509.Certificate) chan error {
	errs := make(chan error, 1)
	go func() {
		pool := x509.NewCertPool()
		pool.AddCert(clientCA)
		nc, err := ln.Accept()
		if err != nil {
			errs <- err
			return
		}
		defer nc.Close()
		tc := tls.Server(nc, &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool})
		hello := make([]byte, 5)
		if _, err := io.ReadFull(tc, hello); err != nil {
			errs <- err
			return
		}
		if string(hello) != "hello" {
			errs <- io.ErrUnexpectedEOF
			return
		}
		if _, err := tc.Write([]byte("world")); err != nil {
			errs <- err
			return
		}
		errs <- tc.Close()
	}()
	return errs
}

func TestHandshakesWithClientCertificate(t *testing.T) {
	ca := rootCA(t, "FDB Test Root")
	serverCert, serverKey := ca.sign(t, leafTemplate(t, leafName()))
	clientCert, clientKey := ca.sign(t, leafTemplate(t, setAttr(leafName(), utf8Attr(oidCN, "fdb-client"))))
	var c tlsConfig
	c.setCertBytes(certPEM(clientCert))
	c.setKeyBytes(keyPEM(t, clientKey))
	c.setCABytes(ca.pem())
	c.setVerifyPeers([]byte("S.CN=nope|S.CN=fdb-server,R.CN=FDB Test Root"))
	cfg, err := c.build()
	if err != nil {
		t.Fatal(err)
	}
	server := tls.Certificate{Certificate: [][]byte{serverCert.Raw}, PrivateKey: serverKey}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	errs := serveOnce(ln, server, ca.cert)
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tc := tls.Client(nc, cfg)
	if _, err := tc.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	reply, err := io.ReadAll(tc)
	if err != nil || string(reply) != "world" {
		t.Fatalf("reply %q: %v", reply, err)
	}
	tc.Close()
	if err := <-errs; err != nil {
		t.Fatalf("server: %v", err)
	}

	c.setVerifyPeers([]byte("S.CN=fdb-client"))
	if cfg, err = c.build(); err != nil {
		t.Fatal(err)
	}
	errs = serveOnce(ln, server, ca.cert)
	nc, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tc = tls.Client(nc, cfg)
	if err := tc.HandshakeContext(context.Background()); err == nil {
		t.Fatal("server rejected by verify-peers was accepted")
	}
	tc.Close()
	if err := <-errs; err == nil {
		t.Fatal("server saw a successful exchange")
	}
}

// withNetworkTLS installs c as the process TLS configuration for one test.
func withNetworkTLS(t *testing.T, c tlsConfig) {
	n := networkConfig()
	n.mu.Lock()
	saved := n.tls
	n.tls, n.client = c, nil
	n.mu.Unlock()
	t.Cleanup(func() {
		n.mu.Lock()
		n.tls, n.client = saved, nil
		n.mu.Unlock()
	})
}

func TestConnectOverTLS(t *testing.T) {
	ca := rootCA(t, "FDB Test Root")
	serverCert, serverKey := ca.sign(t, leafTemplate(t, leafName()))
	clientCert, clientKey := ca.sign(t, leafTemplate(t, setAttr(leafName(), utf8Attr(oidCN, "fdb-client"))))
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	fs := newFakeServer(t, testHandler)
	fs.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{serverCert.Raw}, PrivateKey: serverKey}},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	}
	fs.start()

	var c tlsConfig
	c.setCertBytes(certPEM(clientCert))
	c.setKeyBytes(keyPEM(t, clientKey))
	c.setCABytes(ca.pem())
	c.setVerifyPeers([]byte("S.CN=fdb-server"))
	withNetworkTLS(t, c)
	if !TLSEnabled() {
		t.Fatal("TLS not enabled")
	}
	conn := connect(t, fs)
	for _, dest := range []wire.UID{echoTok, bigTok} {
		if err := call(conn, dest); err != nil {
			t.Fatal(err)
		}
	}
	if err := call(conn, errTok); err != fdberr.FutureVersion {
		t.Fatalf("got %v", err)
	}
	if conn.ProtocolVersion() != wire.Protocol74 || conn.checksum {
		t.Fatal("TLS connection has wrong protocol or enabled checksums")
	}
	checkAdvertisements(t, fs, wire.Protocol74)

	older := newFakeServer(t, testHandler)
	older.tlsConfig = fs.tlsConfig
	older.version = uint64(wire.Protocol73) | objectSerializerFlag
	older.start()
	negotiated := connect(t, older)
	if negotiated.ProtocolVersion() != wire.Protocol73 || negotiated.checksum {
		t.Fatal("negotiated TLS connection has wrong protocol or enabled checksums")
	}
	for _, dest := range []wire.UID{echoTok, bigTok} {
		if err := call(negotiated, dest); err != nil {
			t.Fatalf("request after TLS protocol negotiation: %v", err)
		}
	}
	checkAdvertisements(t, older, wire.Protocol74, wire.Protocol73)

	c.setVerifyPeers([]byte("S.CN=someone-else"))
	withNetworkTLS(t, c)
	if _, err := Connect(context.Background(), fs.addr()); err != NotDelivered {
		t.Fatalf("rejected server: got %v", err)
	}

	c.setDisablePlaintext(true)
	withNetworkTLS(t, c)
	plain := fs.addr()
	plain.TLS = false
	if _, err := Connect(context.Background(), plain); err != NotDelivered {
		t.Fatalf("plaintext with plaintext disabled: got %v", err)
	}
}
