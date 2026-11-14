package fdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestHistoricalVersionstampOperandLimits(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	prefix := testPrefix(t, db)
	probe := mustTr(t, db)
	if _, err := probe.ReadVersion(ctx); err != nil {
		t.Fatal(err)
	}
	probe.Close()
	legacy := serverBefore(t, db, wire.Protocol52)

	for _, tc := range []struct {
		name        string
		key, native bool
	}{
		{name: "key", key: true},
		{name: "value"},
		{name: "key-native", key: true, native: true},
		{name: "value-native", native: true},
	} {
		for _, overflow := range []bool{false, true} {
			name := tc.name
			if overflow {
				name += "-overflow"
			}
			t.Run(name, func(t *testing.T) {
				tr := mustTr(t, db)
				defer tr.Close()
				if tc.native {
					if err := tr.Options().SetReadYourWritesDisable(); err != nil {
						t.Fatal(err)
					}
				}
				var storedKey, expected []byte
				var wantError Error
				if tc.key {
					n := kv.KeySizeLimit + 2
					if overflow {
						n++
					}
					operand := bytes.Repeat([]byte{'k'}, n)
					copy(operand, prefix)
					for i := len(prefix); i < len(prefix)+10; i++ {
						operand[i] = 0xff
					}
					binary.LittleEndian.PutUint32(operand[n-4:], uint32(len(prefix)))
					tr.SetVersionstampedKey(operand, []byte("value"))
					storedKey, expected = operand[:n-4], []byte("value")
					wantError = ErrKeyTooLarge
				} else {
					n := kv.ValueSizeLimit + 4
					if overflow {
						n++
					}
					operand := bytes.Repeat([]byte{'v'}, n)
					for i := range 10 {
						operand[i] = 0xff
					}
					binary.LittleEndian.PutUint32(operand[n-4:], 0)
					storedKey = key(prefix, "value")
					tr.SetVersionstampedValue(storedKey, operand)
					expected = operand[:n-4]
					wantError = ErrValueTooLarge
				}
				err := tr.Commit(ctx)
				if overflow || !legacy {
					if !errors.Is(err, wantError) {
						t.Fatalf("oversize canonical operand = %v; want %v", err, wantError)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				stamp, err := tr.Versionstamp()
				if err != nil {
					t.Fatal(err)
				}
				if tc.key {
					copy(storedKey[len(prefix):], stamp)
				} else {
					copy(expected, stamp)
				}
				read := mustTr(t, db)
				defer read.Close()
				actual, err := read.Get(ctx, storedKey)
				if err != nil || !bytes.Equal(actual, expected) {
					t.Fatalf("stored versionstamp operand: got %d bytes, %v; want %d matching bytes", len(actual), err, len(expected))
				}
			})
		}
	}
}
