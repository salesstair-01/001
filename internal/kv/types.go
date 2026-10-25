// Package kv holds the KV API's types, shared by the query engine's client
// (internal/kvrpc), the SQL engine and the directory. The KV owner itself
// is kvd-rs/ (Rust, on SlateDB); proto/s2z/kv/kv.proto is the API.
package kv

import (
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// TxnID is the query engine's id for a transaction (a UUIDv7).
type TxnID [16]byte

func (id TxnID) String() string { return hex.EncodeToString(id[:]) }

// Isolation levels (D6).
type Isolation int

const (
	RepeatableRead Isolation = iota
	ReadCommitted
)

// MutationOp is the kind of a write.
type MutationOp int

const (
	OpPut        MutationOp = iota // write a value
	OpDelete                       // write a delete marker
	OpInsertOnly                   // write a value; DuplicateKey if a live version exists
)

// Mutation is one write in a Mutate call.
type Mutation struct {
	Op    MutationOp
	Key   []byte // user key
	Value []byte
}

// BeginOptions start a transaction on the owner.
type BeginOptions struct {
	Isolation Isolation
	// ConsistentSnapshot takes the snapshot now (START TRANSACTION WITH
	// CONSISTENT SNAPSHOT) instead of at the first consistent read.
	ConsistentSnapshot bool
}

// LockMode is a row lock mode (D5).
type LockMode int

const (
	LockNone LockMode = iota
	LockShared
	LockExclusive
)

// DefaultLockWait is MySQL's default innodb_lock_wait_timeout.
const DefaultLockWait = 50 * time.Second

// ReadOptions qualify a Get or Scan.
type ReadOptions struct {
	Stmt uint64
	// IncludeStmt makes the statement's own writes visible (duplicate-key
	// checks); otherwise a statement never reads its own output.
	IncludeStmt bool
	// Lock makes this a locking read (UPDATE, DELETE, FOR UPDATE, LOCK IN
	// SHARE MODE): each row is locked and its latest version is read.
	Lock     LockMode
	LockWait time.Duration // 0 = DefaultLockWait
}

// KV is a key and its value, as returned by reads.
type KV struct {
	Key   []byte
	Value []byte
}

// Guard is a commit precondition: the latest committed value of Key must be
// Value (nil = absent). Query engines guard a table's schema version.
type Guard struct {
	Key, Value []byte
}

// TxnState is reported by TxnStatus.
type TxnState int

const (
	TxnUnknown   TxnState = iota // aborted, or never existed (definitive after a takeover)
	TxnActive                    // open on the owner
	TxnCommitted                 // committed, CommitTS set
)

// PartitionEpoch is a served partition and its epoch.
type PartitionEpoch struct {
	Partition, Epoch uint64
}

// ErrKind classifies KV errors; the query engine maps each to a MySQL error (D7).
type ErrKind int

const (
	KindInternal      ErrKind = iota
	KindLockTimeout           // 1205, roll back the statement
	KindDuplicateKey          // 1062, roll back the statement
	KindDeadlock              // 1213, roll back the transaction
	KindEpochChanged          // 1213, roll back the transaction; TxnStatus if a commit was in flight
	KindTxnTooLarge           // 1197, roll back the transaction
	KindTxnNotFound           // the transaction was aborted on the owner (lease expiry, deadlock, owner loss)
	KindUnavailable           // the owner could not reach object storage; outcome of an in-flight commit unknown
	KindSchemaChanged         // a commit guard failed: written with a stale schema; reported as 1213
)

var kindNames = map[ErrKind]string{
	KindInternal:      "Internal",
	KindLockTimeout:   "LockTimeout",
	KindDuplicateKey:  "DuplicateKey",
	KindDeadlock:      "Deadlock",
	KindEpochChanged:  "EpochChanged",
	KindTxnTooLarge:   "TxnTooLarge",
	KindTxnNotFound:   "TxnNotFound",
	KindUnavailable:   "Unavailable",
	KindSchemaChanged: "SchemaChanged",
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

// KindOf returns the kind of err, or KindInternal.
func KindOf(err error) ErrKind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return KindInternal
}
