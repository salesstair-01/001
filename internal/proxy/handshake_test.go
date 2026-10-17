package proxy

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

// The proxy's handshake announces autocommit, as MySQL's does; PyMySQL (and
// so SQLAlchemy) sends SET autocommit = 0 only when it sees the flag.
func TestHandshakeAnnouncesAutocommit(t *testing.T) {
	s := newStack(t, stackOpts{backendIdle: time.Minute, engineIdle: time.Minute})
	conn, err := net.DialTimeout("tcp", s.proxy.Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, int(hdr[0])|int(hdr[1])<<8|int(hdr[2])<<16)
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatal(err)
	}
	i := 1 + bytes.IndexByte(body[1:], 0) + 1 + 4 + 8 + 1 + 2 + 1
	if st := binary.LittleEndian.Uint16(body[i:]); st&0x0002 == 0 {
		t.Fatalf("handshake status %#x lacks SERVER_STATUS_AUTOCOMMIT", st)
	}
}
