package kvrpc

import (
	"reflect"
	"testing"

	"github.com/salesstair-01/001/internal/kv"
)

// Every message survives the protobuf wire unchanged (empty byte slices
// come back nil, as they did with gob).
func TestCodecRoundTrip(t *testing.T) {
	h := Header{Partition: 7, Epoch: 3, Background: true}
	txn := kv.TxnID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	opts := ReadOpts{Stmt: 4, IncludeStmt: true, Lock: kv.LockExclusive, LockWaitMs: 900}
	msgs := []any{
		&Empty{},
		&OpenReq{Partition: 9},
		&OpenResp{Epoch: 12},
		&BeginReq{H: h, Txn: txn, Isolation: kv.ReadCommitted, ConsistentSnapshot: true},
		&GetReq{H: h, Txn: txn, Keys: [][]byte{[]byte("a"), []byte("b")}, Opts: opts},
		// Inside a repeated field an absent value comes back empty; Found
		// tells them apart.
		&GetResp{Values: [][]byte{[]byte("x"), {}}, Found: []bool{true, false}},
		&ScanReq{H: h, Txn: txn, Start: []byte("a"), End: []byte("z"), Limit: 100, Opts: opts},
		&ScanResp{Keys: [][]byte{[]byte("a")}, Values: [][]byte{[]byte("1")}, Resume: []byte("b")},
		&MutateReq{H: h, Txn: txn, Stmt: 2, LockWaitMs: 50, Mutations: []kv.Mutation{
			{Op: kv.OpPut, Key: []byte("k"), Value: []byte("v")},
			{Op: kv.OpDelete, Key: []byte("d")},
			{Op: kv.OpInsertOnly, Key: []byte("i"), Value: []byte("w")},
			{Op: kv.OpPut, Key: []byte("e"), Value: []byte{}}, // an empty value stays a value
		}},
		&StmtReq{H: h, Txn: txn, Stmt: 5},
		&TxnReq{H: h, Txn: txn, Guards: []kv.Guard{{Key: []byte("g"), Value: []byte("1")}}},
		&CommitResp{CommitTS: 99},
		&TxnStatusResp{State: kv.TxnCommitted, CommitTS: 98},
		&PingResp{Owner: "kv-1"},
		&AutoIncReq{H: h, Txn: txn, Table: 1024, Count: 3, AtLeast: 10},
		&AutoIncReq{H: h, Table: 1024}, // zero txn: a peek
		&AutoIncResp{First: 41},
		&HeartbeatReq{H: h, Txns: []kv.TxnID{txn, {}}},
	}
	var c protoCodec
	for _, m := range msgs {
		b, err := c.Marshal(m)
		if err != nil {
			t.Fatalf("%T: %v", m, err)
		}
		got := reflect.New(reflect.TypeOf(m).Elem()).Interface()
		if err := c.Unmarshal(b, got); err != nil {
			t.Fatalf("%T: %v", m, err)
		}
		if !reflect.DeepEqual(got, m) {
			t.Errorf("%T: got %+v, want %+v", m, got, m)
		}
	}
	if _, err := c.Marshal(&struct{}{}); err == nil {
		t.Error("unregistered type marshalled")
	}
}
