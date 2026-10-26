package mysql

import (
	"bytes"
	"context"
	"testing"
)

// s2z patch: an OK packet's info is a string<lenenc>, as MySQL writes it.
func TestOKPacketInfoIsLenEnc(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()
	for _, info := range []string{"Statement prepared", "Records: 2  Duplicates: 0  Warnings: 0"} {
		if err := sConn.writeOKPacketWithInfo(2, 0, 2, 1, info); err != nil {
			t.Fatal(err)
		}
		data, err := cConn.ReadPacket(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		want := append([]byte{OKPacket, 2, 0, 2, 0, 1, 0, byte(len(info))}, info...)
		if !bytes.Equal(data, want) {
			t.Fatalf("OK packet\n got % x\nwant % x", data, want)
		}
		if got := parseOKPacketInfo(data); got != info {
			t.Fatalf("parseOKPacketInfo = %q, want %q", got, info)
		}
	}
}
