package client

import (
	"encoding/binary"
	"unicode/utf8"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/options"
)

// txOption is a decoded transaction option or database option: its code and
// the parameter its type carries.
type txOption struct {
	code int
	// i is the Int parameter.
	i int64
	// s is the String or Bytes parameter.
	s []byte
}

// paramKind is the parameter type an option code takes.
type paramKind uint8

const (
	paramNone paramKind = iota
	paramInt
	paramString
	paramBytes
)

// txParamKinds lists the transaction options decodeTxOption accepts and their parameter kinds.
var txParamKinds = map[int]paramKind{
	options.TransactionCausalWriteRisky:                   paramNone,
	options.TransactionCausalReadRisky:                    paramNone,
	options.TransactionCausalReadDisable:                  paramNone,
	options.TransactionIncludePortInAddress:               paramNone,
	options.TransactionNextWriteNoWriteConflictRange:      paramNone,
	options.TransactionReadYourWritesDisable:              paramNone,
	options.TransactionReadAheadDisable:                   paramNone,
	options.TransactionReadServerSideCacheEnable:          paramNone,
	options.TransactionReadServerSideCacheDisable:         paramNone,
	options.TransactionReadPriorityNormal:                 paramNone,
	options.TransactionReadPriorityLow:                    paramNone,
	options.TransactionReadPriorityHigh:                   paramNone,
	options.TransactionDurabilityDatacenter:               paramNone,
	options.TransactionDurabilityRisky:                    paramNone,
	options.TransactionDurabilityDevNullIsWebScale:        paramNone,
	options.TransactionPrioritySystemImmediate:            paramNone,
	options.TransactionPriorityBatch:                      paramNone,
	options.TransactionInitializeNewDatabase:              paramNone,
	options.TransactionAccessSystemKeys:                   paramNone,
	options.TransactionReadSystemKeys:                     paramNone,
	options.TransactionRawAccess:                          paramNone,
	options.TransactionBypassStorageQuota:                 paramNone,
	options.TransactionDebugRetryLogging:                  paramString,
	options.TransactionTransactionLoggingEnable:           paramString,
	options.TransactionDebugTransactionIdentifier:         paramString,
	options.TransactionLogTransaction:                     paramNone,
	options.TransactionTransactionLoggingMaxFieldLength:   paramInt,
	options.TransactionServerRequestTracing:               paramNone,
	options.TransactionTimeout:                            paramInt,
	options.TransactionRetryLimit:                         paramInt,
	options.TransactionMaxRetryDelay:                      paramInt,
	options.TransactionSizeLimit:                          paramInt,
	options.TransactionAutomaticIdempotency:               paramNone,
	options.TransactionSnapshotRYWEnable:                  paramNone,
	options.TransactionSnapshotRYWDisable:                 paramNone,
	options.TransactionLockAware:                          paramNone,
	options.TransactionUsedDuringCommitProtectionDisable:  paramNone,
	options.TransactionReadLockAware:                      paramNone,
	options.TransactionUseProvisionalProxies:              paramNone,
	options.TransactionReportConflictingKeys:              paramNone,
	options.TransactionSpecialKeySpaceRelaxed:             paramNone,
	options.TransactionSpecialKeySpaceEnableWrites:        paramNone,
	options.TransactionTag:                                paramBytes,
	options.TransactionAutoThrottleTag:                    paramBytes,
	options.TransactionSpanParent:                         paramBytes,
	options.TransactionExpensiveClearCostEstimationEnable: paramNone,
	options.TransactionBypassUnreadable:                   paramNone,
	options.TransactionUseGRVCache:                        paramNone,
	options.TransactionAuthorizationToken:                 paramString,
	options.TransactionEnableReplicaConsistencyCheck:      paramNone,
	options.TransactionConsistencyCheckRequiredReplicas:   paramInt,
}

// dbParamKinds lists the database options decodeDbOption accepts and their parameter kinds.
var dbParamKinds = map[int]paramKind{
	options.DatabaseLocationCacheSize:                            paramInt,
	options.DatabaseMaxWatches:                                   paramInt,
	options.DatabaseMachineID:                                    paramBytes,
	options.DatabaseDatacenterID:                                 paramBytes,
	options.DatabaseSnapshotRYWEnable:                            paramNone,
	options.DatabaseSnapshotRYWDisable:                           paramNone,
	options.DatabaseTransactionLoggingMaxFieldLength:             paramInt,
	options.DatabaseTransactionTimeout:                           paramInt,
	options.DatabaseTransactionRetryLimit:                        paramInt,
	options.DatabaseTransactionMaxRetryDelay:                     paramInt,
	options.DatabaseTransactionSizeLimit:                         paramInt,
	options.DatabaseTransactionCausalReadRisky:                   paramNone,
	options.DatabaseTransactionIncludePortInAddress:              paramNone,
	options.DatabaseTransactionAutomaticIdempotency:              paramNone,
	options.DatabaseTransactionBypassUnreadable:                  paramNone,
	options.DatabaseTransactionUsedDuringCommitProtectionDisable: paramNone,
	options.DatabaseTransactionReportConflictingKeys:             paramNone,
	options.DatabaseUseConfigDatabase:                            paramNone,
	options.DatabaseTestCausalReadRisky:                          paramInt,
}

// dbDefaultFor maps database options with defaultFor to the transaction option they default.
var dbDefaultFor = map[int]int{
	options.DatabaseTransactionLoggingMaxFieldLength:             options.TransactionTransactionLoggingMaxFieldLength,
	options.DatabaseTransactionTimeout:                           options.TransactionTimeout,
	options.DatabaseTransactionRetryLimit:                        options.TransactionRetryLimit,
	options.DatabaseTransactionMaxRetryDelay:                     options.TransactionMaxRetryDelay,
	options.DatabaseTransactionSizeLimit:                         options.TransactionSizeLimit,
	options.DatabaseTransactionCausalReadRisky:                   options.TransactionCausalReadRisky,
	options.DatabaseTransactionIncludePortInAddress:              options.TransactionIncludePortInAddress,
	options.DatabaseTransactionAutomaticIdempotency:              options.TransactionAutomaticIdempotency,
	options.DatabaseTransactionBypassUnreadable:                  options.TransactionBypassUnreadable,
	options.DatabaseTransactionUsedDuringCommitProtectionDisable: options.TransactionUsedDuringCommitProtectionDisable,
	options.DatabaseTransactionReportConflictingKeys:             options.TransactionReportConflictingKeys,
}

// decodeParam checks param against kind (from_raw): Int parameters are 8 bytes
// little-endian, String parameters valid UTF-8; failures are invalid_option_value.
func decodeParam(kind paramKind, code int, param []byte) (txOption, error) {
	o := txOption{code: code}
	switch kind {
	case paramInt:
		if len(param) != 8 {
			return o, fdberr.InvalidOptionValue
		}
		o.i = int64(binary.LittleEndian.Uint64(param))
	case paramString:
		if !utf8.Valid(param) {
			return o, fdberr.InvalidOptionValue
		}
		o.s = append([]byte{}, param...)
	case paramBytes:
		o.s = append([]byte{}, param...)
	}
	return o, nil
}

// decodeTxOption decodes a raw transaction option code and parameter.
func decodeTxOption(code int, param []byte) (txOption, error) {
	kind, ok := txParamKinds[code]
	if !ok {
		return txOption{}, fdberr.InvalidOption
	}
	return decodeParam(kind, code, param)
}

// decodeDbOption decodes a raw database option code and parameter; the result's code is the database option's.
func decodeDbOption(code int, param []byte) (txOption, error) {
	kind, ok := dbParamKinds[code]
	if !ok {
		return txOption{}, fdberr.InvalidOption
	}
	return decodeParam(kind, code, param)
}

// persistent reports whether the option survives reset and on_error.
func (o *txOption) persistent() bool {
	switch o.code {
	case options.TransactionTimeout, options.TransactionRetryLimit, options.TransactionMaxRetryDelay,
		options.TransactionAuthorizationToken:
		return true
	}
	return false
}

// transactionDefault converts a database option with defaultFor into the
// transaction option it defaults, validated as setting it on a transaction would.
func transactionDefault(dbOpt txOption, code int) (txOption, error) {
	topt := txOption{code: code, i: dbOpt.i}
	if err := validateTransactionOption(&topt); err != nil {
		return txOption{}, err
	}
	return topt, nil
}

// validateTransactionOption applies the range checks setOption applies to integer
// transaction options.
func validateTransactionOption(o *txOption) error {
	const maxInt32 = 1<<31 - 1
	v := o.i
	ok := true
	switch o.code {
	case options.TransactionTimeout:
		ok = v >= 0 && v <= maxInt32
	case options.TransactionRetryLimit:
		ok = v >= -1 && v <= maxInt32
	case options.TransactionMaxRetryDelay:
		ok = v >= 0 && v <= maxInt32
	case options.TransactionSizeLimit:
		ok = v >= 32 && v <= transactionSizeLimit
	case options.TransactionTransactionLoggingMaxFieldLength:
		ok = v != 0 && v >= -1 && v <= maxInt32
	}
	if !ok {
		return fdberr.InvalidOptionValue
	}
	return nil
}

// addTag adds tag to tags: at most 5 tags of at most 16 bytes each.
func addTag(tags *[][]byte, tag []byte) error {
	if len(tag) > 16 {
		return fdberr.TagTooLong
	}
	if len(*tags) >= 5 {
		return fdberr.TooManyTags
	}
	for _, t := range *tags {
		if string(t) == string(tag) {
			return nil
		}
	}
	*tags = append(*tags, tag)
	return nil
}

// Streaming modes as C values.
const (
	modeWantAll  = options.StreamingModeWantAll
	modeIterator = options.StreamingModeIterator
	modeExact    = options.StreamingModeExact
	modeSerial   = options.StreamingModeSerial
)

// RangeOpts are range read parameters with C API semantics (fdb_transaction_get_range).
type RangeOpts struct {
	// Limit is the maximum number of rows, 0 = unlimited.
	Limit int
	// TargetBytes is the maximum number of bytes, 0 = unlimited.
	TargetBytes int
	// Mode is the C StreamingMode value.
	Mode int
	// Iteration is the 1-based iteration counter for the iterator mode.
	Iteration int
	// Reverse reads from the end of the range backward.
	Reverse bool
}

// modeBytes is the byte limit of each streaming mode from exact to serial.
var modeBytes = [5]int{kv.Unlimited, 256, 1000, 4096, 80000}

// iterationProgression is the byte limit of each iteration of the iterator mode.
var iterationProgression = [10]int{4096, 6144, 9216, 13824, 20736, 31104, 46656, 69984, 80000, 120000}

// limits validates the C API range parameters and converts them to kv.Limits.
func (o RangeOpts) limits() (kv.Limits, error) {
	limit, target := o.Limit, o.TargetBytes
	if limit == 0 {
		limit = kv.Unlimited
	}
	if target == 0 {
		target = kv.Unlimited
	}
	if limit == kv.Unlimited && target == kv.Unlimited && o.Mode == modeExact {
		return kv.Limits{}, fdberr.ExactModeWithoutLimits
	}
	mode := o.Mode
	if mode == modeWantAll {
		mode = modeSerial
	}
	var mb int
	if mode == modeIterator {
		if o.Iteration <= 0 {
			return kv.Limits{}, fdberr.ClientInvalidOperation
		}
		mb = iterationProgression[min(o.Iteration, len(iterationProgression))-1]
	} else {
		if mode < 0 || mode > 4 {
			return kv.Limits{}, fdberr.ClientInvalidOperation
		}
		mb = modeBytes[mode]
	}
	if target == kv.Unlimited {
		target = mb
	} else if mb != kv.Unlimited {
		target = min(target, mb)
	}
	return kv.NewLimits(limit, target), nil
}
