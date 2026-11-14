package client

import (
	"testing"

	"github.com/can1357/fdbgo/internal/options"
)

func TestCausalReadDisableDoesNotChangeRiskyReads(t *testing.T) {
	tr := localTxn(t)
	if err := tr.SetOption(options.TransactionCausalReadDisable, nil); err != nil {
		t.Fatal(err)
	}
	if tr.native.Opts.CausalReadRisky {
		t.Fatal("causal_read_disable enabled risky GRV requests")
	}
	if err := tr.SetOption(options.TransactionCausalReadRisky, nil); err != nil {
		t.Fatal(err)
	}
	if err := tr.SetOption(options.TransactionCausalReadDisable, nil); err != nil {
		t.Fatal(err)
	}
	if !tr.native.Opts.CausalReadRisky {
		t.Fatal("causal_read_disable changed an explicit risky setting")
	}
}
