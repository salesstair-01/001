package kvrpc

import (
	"fmt"
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
)

// The KV and directory APIs keep their Go request and response structs; on
// the wire they are the protobuf messages of proto/s2z (kvpb, dirpb), so an
// owner written in another language can serve them. protoCodec converts
// between the two through converters registered per Go type.

type converter struct {
	newPB func() proto.Message
	to    func(any) proto.Message
	from  func(proto.Message, any)
}

var (
	convMu sync.RWMutex
	convs  = map[reflect.Type]converter{}
)

// Message registers how a Go message type *T maps to its protobuf message P.
func Message[T any, P proto.Message](newPB func() P, to func(*T) P, from func(P, *T)) {
	convMu.Lock()
	defer convMu.Unlock()
	convs[reflect.TypeOf((*T)(nil))] = converter{
		newPB: func() proto.Message { return newPB() },
		to:    func(v any) proto.Message { return to(v.(*T)) },
		from:  func(m proto.Message, v any) { from(m.(P), v.(*T)) },
	}
}

func converterOf(v any) (converter, error) {
	convMu.RLock()
	defer convMu.RUnlock()
	c, ok := convs[reflect.TypeOf(v)]
	if !ok {
		return c, fmt.Errorf("kvrpc: no protobuf message registered for %T", v)
	}
	return c, nil
}

// protoCodec is gRPC's "proto" codec over the registered converters.
type protoCodec struct{}

func (protoCodec) Name() string { return "proto" }

func (protoCodec) Marshal(v any) ([]byte, error) {
	if m, ok := v.(proto.Message); ok {
		return proto.Marshal(m)
	}
	c, err := converterOf(v)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(c.to(v))
}

func (protoCodec) Unmarshal(data []byte, v any) error {
	if m, ok := v.(proto.Message); ok {
		return proto.Unmarshal(data, m)
	}
	c, err := converterOf(v)
	if err != nil {
		return err
	}
	m := c.newPB()
	if err := proto.Unmarshal(data, m); err != nil {
		return err
	}
	c.from(m, v)
	return nil
}
