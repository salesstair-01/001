package kv

import (
	"encoding/binary"

	"github.com/salesstair-01/001/internal/encoding"
)

// AUTO_INCREMENT (D7): the owner hands out values from a per-table counter.
// The counter lives in memory and is written to system table 3 with every
// commit of a transaction that used it, so values are never reused after a
// restart (as in MySQL 8) and a rollback doesn't hand values out twice.

func (p *Partition) autoIncKey(table uint32) []byte {
	return encoding.AppendUint(encoding.Prefix(p.id, encoding.TableAutoIncrement, 0), uint64(table), false)
}

// autoIncLocked returns the next value for table, loading it on first use. Requires p.mu.
func (p *Partition) autoIncLocked(table uint32) (uint64, error) {
	if n, ok := p.autoinc[table]; ok {
		return n, nil
	}
	v, _, err := p.latestLocked(p.autoIncKey(table))
	if err != nil {
		return 0, err
	}
	n := uint64(1)
	if len(v) == 8 {
		n = binary.BigEndian.Uint64(v)
	}
	p.autoinc[table] = n
	return n, nil
}

// AutoIncrement reserves count consecutive values for table and returns the
// first. atLeast raises the counter first (an explicit value was inserted, or
// ALTER TABLE ... AUTO_INCREMENT); the counter never goes down. With count 0
// it only raises the counter and returns the next value. A zero txn id peeks.
func (p *Partition) AutoIncrement(epoch uint64, id TxnID, table uint32, count, atLeast uint64) (uint64, error) {
	if err := p.checkEpoch(epoch); err != nil {
		return 0, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopErr != nil {
		return 0, p.stopErr
	}
	var t *Txn
	if id != (TxnID{}) {
		var err error
		if t, err = p.txnLocked(id); err != nil {
			return 0, err
		}
	}
	next, err := p.autoIncLocked(table)
	if err != nil {
		return 0, err
	}
	if t == nil {
		return next, nil
	}
	if atLeast > next {
		next = atLeast
	}
	first := next
	p.autoinc[table] = next + count
	if t.autoinc == nil {
		t.autoinc = map[uint32]bool{}
	}
	t.autoinc[table] = true
	return first, nil
}

// autoIncWritesLocked returns the counter writes a committing transaction
// carries. Requires p.mu.
func (p *Partition) autoIncWritesLocked(t *Txn) map[string][]byte {
	out := map[string][]byte{}
	for table := range t.autoinc {
		out[string(p.autoIncKey(table))] = binary.BigEndian.AppendUint64(nil, p.autoinc[table])
	}
	return out
}
