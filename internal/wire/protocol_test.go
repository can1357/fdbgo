package wire

import "testing"

func TestProtocolCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name       string
		version    ProtocolVersion
		normalized ProtocolVersion
		supported  bool
	}{
		{"5.0", Protocol50, 0x0fdb00a550010000, true},
		{"5.1", Protocol51, 0x0fdb00a551040000, true},
		{"5.2", Protocol52, 0x0fdb00a552000000, true},
		{"6.0", Protocol60, 0x0fdb00a570010000, true},
		{"6.1", Protocol61, 0x0fdb00b061060000, true},
		{"6.2", Protocol62, 0x0fdb00b062010000, true},
		{"6.3", Protocol63, 0x0fdb00b063010000, true},
		{"7.0", Protocol70, 0x0fdb00b070010000, true},
		{"7.1", Protocol71, 0x0fdb00b071010000, true},
		{"8.0", Protocol80, 0x0fdb00b080000000, true},
		{"7.3", Protocol73, Protocol73, true},
		{"7.4", Protocol74, Protocol74, true},
		{"7.3 patch and flags", Protocol73 | 0x1000_0000_0000_ffff, Protocol73, true},
		{"7.4 patch and flags", Protocol74 | 0xf000_0000_0000_abcd, Protocol74, true},
		{"zero", 0, 0, false},
		{"flags only", 0x1000_0000_0000_0000, 0, false},
		{"7.2", Protocol72, 0x0fdb_00b0_7200_0000, true},
		{"7.0 unknown feature", 0x0fdb00b070020001, 0x0fdb00b070020000, false},
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
			for _, supported := range [...]ProtocolVersion{Protocol50, Protocol51, Protocol52, Protocol60, Protocol61, Protocol62, Protocol63, Protocol70, Protocol71, Protocol72, Protocol73, Protocol74, Protocol80} {
				want := tc.normalized == supported.Normalized()
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
