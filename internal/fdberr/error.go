// Package fdberr defines the FoundationDB error code type shared by every
// layer of the client. The public package fdb re-exports it as fdb.Error.
package fdberr

import "strconv"

// Error is a FoundationDB error code. Like syscall.Errno it is a comparable
// value, so errors.Is(err, fdb.ErrNotCommitted) matches by code.
type Error int

// Error returns the code's description, as fdb_get_error does: codes outside
// [0, 30000] report unknown_error's text, unlisted codes "UNKNOWN_ERROR".
func (e Error) Error() string {
	code := e
	if code < 0 || code > 30000 {
		code = UnknownError
	}
	if _, desc, ok := lookup(code); ok {
		return desc
	}
	return "UNKNOWN_ERROR"
}

// Name returns the snake_case error name, or
// "unknown_error" for unlisted codes.
func (e Error) Name() string {
	if name, _, ok := lookup(e); ok {
		return name
	}
	return "unknown_error"
}

// GoString formats the error for %#v as name(code).
func (e Error) GoString() string { return e.Name() + "(" + strconv.Itoa(int(e)) + ")" }

// MaybeCommitted reports whether the transaction may have committed despite
// the error (fdb_error_predicate MAYBE_COMMITTED).
func (e Error) MaybeCommitted() bool {
	return e == CommitUnknownResult || e == ClusterVersionChanged
}

// Retryable reports whether retrying the transaction may succeed
// (fdb_error_predicate RETRYABLE).
func (e Error) Retryable() bool { return e.MaybeCommitted() || e.RetryableNotCommitted() }

// RetryableNotCommitted reports whether the transaction certainly did not
// commit and retrying may succeed (fdb_error_predicate
// RETRYABLE_NOT_COMMITTED).
func (e Error) RetryableNotCommitted() bool {
	switch e {
	case NotCommitted, TransactionTooOld, FutureVersion, DatabaseLocked,
		GRVProxyMemoryLimitExceeded, CommitProxyMemoryLimitExceeded,
		TransactionThrottledHotShard, BatchTransactionThrottled, ProcessBehind,
		TagThrottled, ProxyTagThrottled, TransactionRejectedRangeLocked:
		return true
	}
	return false
}
