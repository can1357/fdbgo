package fdb

// Tests of error wrapping and Transact retry behavior.

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

// layerError wraps an Error as its cause, like a layer's typed error.
type layerError struct{ cause error }

func (e *layerError) Error() string { return "layer error: " + e.cause.Error() }
func (e *layerError) Unwrap() error { return e.cause }

// errInvalidDocument is a domain error with no Error in its chain.
var errInvalidDocument = errors.New("invalid document")

// TestErrorChain checks that an Error is found through wrapping, and not
// in a stringified copy.
func TestErrorChain(t *testing.T) {
	var fe Error
	if err := error(&layerError{Error(1020)}); !errors.As(err, &fe) || fe != ErrNotCommitted {
		t.Errorf("layer error: %v", fe)
	}
	if err := fmt.Errorf("outer: %w", &layerError{Error(1020)}); !errors.Is(err, ErrNotCommitted) {
		t.Errorf("nested: %v", err)
	}
	if err := errors.New((&layerError{Error(1020)}).Error()); errors.As(err, &fe) {
		t.Errorf("stringified error still has an Error: %v", fe)
	}
	if s := fmt.Sprintf("%v %#v", Error(1020), Error(1020)); s == "" {
		t.Error("empty formatting")
	}
}

// TestTransactRetriesWrappedError checks that Transact retries a retryable
// Error wrapped by the closure.
func TestTransactRetriesWrappedError(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "run_retry_wrapped")
	var attempt atomic.Int32
	err := db.Transact(ctx, func(tr *Transaction) error {
		if attempt.Add(1) == 1 {
			return fmt.Errorf("wrapped: %w", &layerError{ErrNotCommitted})
		}
		tr.Set(k, []byte("ok"))
		return nil
	})
	if err != nil || attempt.Load() != 2 {
		t.Fatalf("transact = %v after %d attempts; want success after 2", err, attempt.Load())
	}
}

// TestTransactDomainErrorNotRetried checks that a domain error ends
// Transact after one attempt and is returned as is.
func TestTransactDomainErrorNotRetried(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	var attempt atomic.Int32
	err := db.Transact(ctx, func(tr *Transaction) error {
		attempt.Add(1)
		return errInvalidDocument
	})
	if err != errInvalidDocument || attempt.Load() != 1 {
		t.Fatalf("transact = %v after %d attempts", err, attempt.Load())
	}
}

// TestNonRetryableErrorIsReturned checks OnError hands back errors it does
// not retry.
func TestNonRetryableErrorIsReturned(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	var attempt atomic.Int32
	err := db.Transact(ctx, func(tr *Transaction) error {
		attempt.Add(1)
		return ErrKeyOutsideLegalRange
	})
	if !errors.Is(err, ErrKeyOutsideLegalRange) || attempt.Load() != 1 {
		t.Fatalf("transact = %v after %d attempts", err, attempt.Load())
	}
}

// customError is a domain error returned from a Transact closure.
type customError struct{}

func (customError) Error() string { return "MyError1" }

// TestTransactReturnsWrappedDomainError checks that Transact returns a domain error
// unchanged.
func TestTransactReturnsWrappedDomainError(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	err := db.Transact(ctx, func(tr *Transaction) error { return fmt.Errorf("custom: %w", customError{}) })
	var ce customError
	if !errors.As(err, &ce) {
		t.Fatalf("transact = %v, want customError", err)
	}
}

// TestTransactFromGoroutine checks Transact from another goroutine.
func TestTransactFromGoroutine(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	done := make(chan error)
	go func() { done <- db.Transact(ctx, func(*Transaction) error { return nil }) }()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestEmptyCommitFromGoroutine checks an empty commit from another goroutine.
func TestEmptyCommitFromGoroutine(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	done := make(chan error)
	go func() {
		tr, err := db.CreateTransaction()
		if err != nil {
			done <- err
			return
		}
		done <- tr.Commit(ctx)
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// conflictingRun reads the subspace, writes it,
// and has another transaction write it too while conflict is true.
func conflictingRun(t *testing.T, db *Database, tr *Transaction, p []byte, conflict bool) error {
	ctx := testCtx(t)
	r := prefixRange(t, p)
	if err := tr.AddWriteConflictRange(r); err != nil {
		return err
	}
	if _, err := sliceRange(ctx, tr, r, RangeOptions{}); err != nil {
		return err
	}
	tr.Set(key(p, "1"), []byte("42"))
	if conflict {
		other, err := db.CreateTransaction()
		if err != nil {
			return err
		}
		other.Set(key(p, "2"), []byte("42"))
		if err := other.Commit(ctx); err != nil {
			t.Fatalf("conflicting transaction: %v", err)
		}
	}
	return nil
}

// TestTransactRetriesConflicts checks that Transact retries 16 conflicts, then commits.
func TestTransactRetriesConflicts(t *testing.T) {
	const limitRetry = 16
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	var counter atomic.Int32
	err := db.Transact(ctx, func(tr *Transaction) error {
		n := counter.Add(1) - 1
		return conflictingRun(t, db, tr, p, n < limitRetry)
	})
	if err != nil || counter.Load() != limitRetry+1 {
		t.Fatalf("transact = %v after %d attempts", err, counter.Load())
	}
}

// TestTransactionRetryLimitOption checks that the retry limit ends the retries.
func TestTransactionRetryLimitOption(t *testing.T) {
	const limitRetry = 16
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	var counter atomic.Int32
	err := db.Transact(ctx, func(tr *Transaction) error {
		if err := tr.Options().SetRetryLimit(limitRetry); err != nil {
			return err
		}
		counter.Add(1)
		return conflictingRun(t, db, tr, p, true)
	})
	if !errors.Is(err, ErrNotCommitted) || counter.Load() != limitRetry+1 {
		t.Fatalf("transact = %v after %d attempts", err, counter.Load())
	}
}
