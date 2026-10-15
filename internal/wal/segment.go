// Package wal defines the object-store WAL segment format (D4). One segment
// holds one commit group; it is written once with put-if-absent at
// p/<partition>/wal/<seq> and never changed.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

var (
	ErrBadSegment = errors.New("wal: corrupt segment")
	castagnoli    = crc32.MakeTable(crc32.Castagnoli)
	magic         = [4]byte{'S', '2', 'Z', 'W'}
)

const formatVersion = 1

// Write is one key/value written by a transaction. Value is a row-v2 value,
// a delete marker (0x81), or a system-table value; Key is the user key
// without the commit_ts suffix.
type Write struct {
	Key   []byte
	Value []byte
}

// Record is one committed transaction.
type Record struct {
	TxnID      [16]byte
	CommitTS   uint64
	WallMicros int64 // wall-clock time, so point-in-time restore can map a date to a commit_ts
	Writes     []Write
}

// Segment is one WAL object. A fence segment has no records.
type Segment struct {
	Epoch   uint64
	Seq     uint64
	Records []Record
}

// Encode serialises a segment with a trailing CRC-32C over everything before it.
func (s *Segment) Encode() []byte {
	n := 4 + 1 + 8 + 8 + 4 + 4
	for _, r := range s.Records {
		n += 16 + 8 + 8 + 4
		for _, w := range r.Writes {
			n += 8 + len(w.Key) + len(w.Value)
		}
	}
	b := make([]byte, 0, n)
	b = append(b, magic[:]...)
	b = append(b, formatVersion)
	b = binary.BigEndian.AppendUint64(b, s.Epoch)
	b = binary.BigEndian.AppendUint64(b, s.Seq)
	b = binary.BigEndian.AppendUint32(b, uint32(len(s.Records)))
	for _, r := range s.Records {
		b = append(b, r.TxnID[:]...)
		b = binary.BigEndian.AppendUint64(b, r.CommitTS)
		b = binary.BigEndian.AppendUint64(b, uint64(r.WallMicros))
		b = binary.BigEndian.AppendUint32(b, uint32(len(r.Writes)))
		for _, w := range r.Writes {
			b = binary.BigEndian.AppendUint32(b, uint32(len(w.Key)))
			b = append(b, w.Key...)
			b = binary.BigEndian.AppendUint32(b, uint32(len(w.Value)))
			b = append(b, w.Value...)
		}
	}
	return binary.BigEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// Checksum returns the CRC stored at the end of an encoded segment.
func Checksum(b []byte) (uint32, error) {
	if len(b) < 4 {
		return 0, ErrBadSegment
	}
	return binary.BigEndian.Uint32(b[len(b)-4:]), nil
}

// Decode parses and verifies a segment.
func Decode(b []byte) (*Segment, error) {
	if len(b) < 29 {
		return nil, ErrBadSegment
	}
	body, sum := b[:len(b)-4], binary.BigEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrBadSegment)
	}
	if [4]byte(body[:4]) != magic || body[4] != formatVersion {
		return nil, fmt.Errorf("%w: bad header", ErrBadSegment)
	}
	r := reader{b: body[5:]}
	s := &Segment{Epoch: r.u64(), Seq: r.u64()}
	nrec := r.u32()
	for i := uint32(0); i < nrec && r.err == nil; i++ {
		var rec Record
		copy(rec.TxnID[:], r.bytes(16))
		rec.CommitTS = r.u64()
		rec.WallMicros = int64(r.u64())
		nw := r.u32()
		for j := uint32(0); j < nw && r.err == nil; j++ {
			k := r.bytes(int(r.u32()))
			v := r.bytes(int(r.u32()))
			rec.Writes = append(rec.Writes, Write{Key: k, Value: v})
		}
		s.Records = append(s.Records, rec)
	}
	if r.err != nil || len(r.b) != 0 {
		return nil, ErrBadSegment
	}
	return s, nil
}

type reader struct {
	b   []byte
	err error
}

func (r *reader) bytes(n int) []byte {
	if r.err != nil || n < 0 || n > len(r.b) {
		r.err = ErrBadSegment
		return nil
	}
	out := r.b[:n:n]
	r.b = r.b[n:]
	return out
}

func (r *reader) u32() uint32 {
	b := r.bytes(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

func (r *reader) u64() uint64 {
	b := r.bytes(8)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// Size returns the encoded size of a record, for the group-commit byte cap.
func (r *Record) Size() int {
	n := 16 + 8 + 8 + 4
	for _, w := range r.Writes {
		n += 8 + len(w.Key) + len(w.Value)
	}
	return n
}
