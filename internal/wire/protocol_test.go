package wire

import "testing"

func TestProtocolCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name       string
		version    ProtocolVersion
		normalized ProtocolVersion
		supported  bool
	}{
		{"7.3", Protocol73, Protocol73, true},
		{"7.4", Protocol74, Protocol74, true},
		{"7.3 patch and flags", Protocol73 | 0x1000_0000_0000_ffff, Protocol73, true},
		{"7.4 patch and flags", Protocol74 | 0xf000_0000_0000_abcd, Protocol74, true},
		{"zero", 0, 0, false},
		{"flags only", 0x1000_0000_0000_0000, 0, false},
		{"7.2", 0x0fdb_00b0_7200_0000, 0x0fdb_00b0_7200_0000, false},
		{"future", 0x0fdb_00b0_7500_0000, 0x0fdb_00b0_7500_0000, false},
		{"7.3 unknown feature", Protocol73 | 0x0001_0000, Protocol73 | 0x0001_0000, false},
		{"7.4 unknown feature", Protocol74 | 0x0001_0000, Protocol74 | 0x0001_0000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.version.Normalized(); got != tc.normalized {
				t.Fatalf("Normalized() = %#x, want %#x", got, tc.normalized)
			}
			if got := tc.version.Supported(); got != tc.supported {
				t.Fatalf("Supported() = %v, want %v", got, tc.supported)
			}
			for _, supported := range [...]ProtocolVersion{Protocol73, Protocol74} {
				want := tc.normalized == supported
				if got := tc.version.Compatible(supported); got != want {
					t.Errorf("Compatible(%#x) = %v, want %v", supported, got, want)
				}
				if got := supported.Compatible(tc.version); got != want {
					t.Errorf("%#x.Compatible(%#x) = %v, want %v", supported, tc.version, got, want)
				}
			}
		})
	}
}
