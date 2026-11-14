package native

import (
	"encoding/json"
	"net/netip"
	"slices"
	"testing"

	"github.com/can1357/fdbgo/internal/cluster"
)

func TestPickCoordinatorAddressUsesAllPreferredDNSAnswers(t *testing.T) {
	ipv4a := netip.MustParseAddr("192.0.2.1")
	ipv4b := netip.MustParseAddr("192.0.2.2")
	ipv6a := netip.MustParseAddr("2001:db8::1")
	ipv6b := netip.MustParseAddr("2001:db8::2")
	mapped := netip.MustParseAddr("::ffff:192.0.2.2")
	for _, tt := range []struct {
		name    string
		answers []netip.Addr
		want    []netip.Addr
	}{
		{"ipv4", []netip.Addr{ipv4a, ipv4b}, []netip.Addr{ipv4a, ipv4b}},
		{"ipv6", []netip.Addr{ipv6a, ipv6b}, []netip.Addr{ipv6a, ipv6b}},
		{"prefer-ipv6", []netip.Addr{ipv4a, ipv6a, ipv4b, ipv6b}, []netip.Addr{ipv6a, ipv6b}},
		{"unmap-ipv4", []netip.Addr{ipv4a, mapped}, []netip.Addr{ipv4a, ipv4b}},
		{"mapped-is-not-ipv6", []netip.Addr{mapped, ipv6a}, []netip.Addr{ipv6a}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			original := slices.Clone(tt.answers)
			// DNS answers are unchanged between retries. A failed first address
			// must not prevent selection of every other preferred-family address.
			for retry, want := range tt.want {
				calls := 0
				got := pickCoordinatorAddress(tt.answers, func(n int) int {
					calls++
					if n != len(tt.want) {
						t.Fatalf("selection has %d alternatives; want %d", n, len(tt.want))
					}
					return retry
				})
				if got != want || calls != 1 {
					t.Fatalf("retry %d selected %v with %d draws; want %v with one draw", retry, got, calls, want)
				}
				if !slices.Equal(tt.answers, original) {
					t.Fatal("selection changed cached DNS answers")
				}
			}
		})
	}
}

func TestDbContextInitializesMemoryRecord(t *testing.T) {
	// An invalid literal address is rejected locally by net.Dialer, so even
	// if the monitor runs before Close, this test does not access the network.
	record := cluster.ConnectionRecord{Conn: cluster.ConnectionString{
		Key:          "memory:context",
		Coordinators: []cluster.Coordinator{{}},
	}}
	first := NewDbContext(record, false)
	first.Close()
	location := first.Record().Location()
	if len(location) != 32 || location == first.ConnectionString().String() {
		t.Fatalf("database did not initialize a memory UID: %q", location)
	}
	second := NewDbContext(record, false)
	second.Close()
	if second.Record().Location() == location {
		t.Fatal("independent databases share a memory location")
	}
	copied := NewDbContext(first.Record(), false)
	copied.Close()
	if copied.Record().Location() != location {
		t.Fatal("database initialization changed an existing record's location")
	}
}

func TestStatusJSONUsesStableMemoryLocation(t *testing.T) {
	// No coordinators means no probes and no network activity.
	record := cluster.ConnectionRecord{Conn: cluster.ConnectionString{Key: "memory:original"}}
	record.Init()
	db := &DbContext{record: record}
	for _, key := range []string{"memory:original", "memory:forwarded"} {
		db.record.Persist(cluster.ConnectionString{Key: key})
		text, err := StatusJSON(t.Context(), db)
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Client struct {
				ClusterFile struct {
					Path     string `json:"path"`
					UpToDate bool   `json:"up_to_date"`
				} `json:"cluster_file"`
			} `json:"client"`
		}
		if err := json.Unmarshal(text, &doc); err != nil {
			t.Fatal(err)
		}
		if got := doc.Client.ClusterFile; got.Path != record.Location() || !got.UpToDate {
			t.Fatalf("status cluster_file = %+v; want stable memory location %q, up to date", got, record.Location())
		}
	}
}
