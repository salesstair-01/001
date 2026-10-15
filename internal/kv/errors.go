// Package kv is the KV owner: one Pebble instance per partition with its WAL
// disabled, plus the object-store durability layer (WAL segments, checkpoints,
// numbered manifests and fencing, D4) and the transaction layer (D5–D7).
package kv

import (
	"errors"
	"fmt"
)

// ErrKind classifies KV errors; the query engine maps each to a MySQL error (D7).
type ErrKind int

const (
	KindInternal     ErrKind = iota
	KindLockTimeout          // 1205, roll back the statement
	KindDuplicateKey         // 1062, roll back the statement
	KindDeadlock             // 1213, roll back the transaction
	KindEpochChanged         // 1213, roll back the transaction; TxnStatus if a commit was in flight
	KindTxnTooLarge          // 1197, roll back the transaction
	KindTxnNotFound          // the transaction was aborted on the owner (lease expiry, deadlock, owner loss)
	KindUnavailable          // the owner could not reach object storage; outcome of an in-flight commit unknown
)

var kindNames = map[ErrKind]string{
	KindInternal:     "Internal",
	KindLockTimeout:  "LockTimeout",
	KindDuplicateKey: "DuplicateKey",
	KindDeadlock:     "Deadlock",
	KindEpochChanged: "EpochChanged",
	KindTxnTooLarge:  "TxnTooLarge",
	KindTxnNotFound:  "TxnNotFound",
	KindUnavailable:  "Unavailable",
}

func (k ErrKind) String() string { return kindNames[k] }

// KindFromString parses ErrKind.String.
func KindFromString(s string) ErrKind {
	for k, n := range kindNames {
		if n == s {
			return k
		}
	}
	return KindInternal
}

// Error is a classified KV error.
type Error struct {
	Kind ErrKind
	Msg  string
	// Key is the conflicting user key for DuplicateKey.
	Key []byte
}

func (e *Error) Error() string { return fmt.Sprintf("kv %s: %s", e.Kind, e.Msg) }

func newErr(kind ErrKind, format string, args ...any) *Error {
	return &Error{Kind: kind, Msg: fmt.Sprintf(format, args...)}
}

// KindOf returns the kind of err, or KindInternal.
func KindOf(err error) ErrKind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return KindInternal
}

var (
	errFenced          = newErr(KindEpochChanged, "partition fenced by a newer owner")
	errStopped         = newErr(KindEpochChanged, "partition no longer served by this owner")
	errTakeoverLost    = errors.New("kv: another node won the takeover")
	errCommitAbandoned = newErr(KindUnavailable, "object store unreachable; commit outcome unknown")
)
