// Package ryw is the read-your-writes layer above the native transaction: a
// write map of pending mutations, a snapshot cache of values read, an iterator
// merging the two, atomic operation evaluation, and the deferred-error behaviour
// of libfdb_c transactions.
//
// A Ryw holds one attempt's state and is safe for concurrent use. Writes are
// synchronous and record a deferred error. Reads validate and pin their
// write-map view at issue time, and synchronously start read-version
// acquisition before returning if they need snapshot I/O. A read returns
// either its result or a blocking wait function; callers must start every
// non-nil wait once, including when dropping the result. Batched reads register
// all operations before starting their waits concurrently. A read's
// stack is retained while snapshot operations block, without replaying work.
// Reads in flight when an attempt ends fail with its error; Commit waits for
// pending reads and watch setups even when their results are discarded.
// Cancelling a read's context finishes its tracking before the wait returns;
// a failed tracked read prevents the attempt from committing.
//
// Read conflicts do not need persistent views. They accumulate in a range
// buffer, coalesced for commit and conflict-boundary reporting, and before
// buffer growth to bound duplicate retention. Approximate size still counts
// each read's conflict contribution, independently of coalescing.
// Commit reuses immutable write-map mutation bytes rather than copying them
// again; caller-owned inputs are still copied when the write is issued.
//
// With read_your_writes_disable, writes and conflict ranges go straight to
// the native lists and reads issued through NativeRead keep the same attempt and
// commit bookkeeping.
//
// Byte slices follow the kv convention (nil = absent, non-nil empty = present
// empty). Keys and values passed in are copied; slices handed out (values,
// keys, rows, conflict boundaries) are shared with the transaction state and
// must not be modified.
//
// readTracker counts reads that commit waits for and keeps the earliest
// failure. Pending-read waits capture only operations already issued; later
// reads neither prolong those waits nor contribute their errors. Successful
// synchronous tracked reads also prevent disabling read-your-writes.
// AttemptEnd closes a channel and cancels blocking snapshot calls;
// generation numbers prevent stale reads from modifying a new attempt. An
// armed watch outlives the attempt that registered it.
package ryw
