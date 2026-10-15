package wal

import (
	"bytes"
	"testing"
)

func TestSegmentRoundTrip(t *testing.T) {
	s := &Segment{Epoch: 3, Seq: 42, Records: []Record{
		{TxnID: [16]byte{1}, CommitTS: 7, WallMicros: 1234, Writes: []Write{{Key: []byte("k1"), Value: []byte("v1")}, {Key: []byte("k2"), Value: []byte{0x81}}}},
		{TxnID: [16]byte{2}, CommitTS: 8, Writes: nil},
	}}
	b := s.Encode()
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Epoch != 3 || got.Seq != 42 || len(got.Records) != 2 || got.Records[0].CommitTS != 7 ||
		!bytes.Equal(got.Records[0].Writes[1].Value, []byte{0x81}) || got.Records[1].TxnID[0] != 2 {
		t.Fatalf("mismatch: %+v", got)
	}
	b[10] ^= 1
	if _, err := Decode(b); err == nil {
		t.Fatal("corruption not detected")
	}
	fence := (&Segment{Epoch: 1, Seq: 5}).Encode()
	if f, err := Decode(fence); err != nil || len(f.Records) != 0 {
		t.Fatal("fence segment")
	}
}
