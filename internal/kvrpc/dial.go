package kvrpc

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Dial connects to a KV node without TLS (v1 runs inside one network).
func Dial(addr string) (*Client, error) {
	cc, err := grpc.NewClient(addr, append(DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		return nil, err
	}
	return NewClient(cc), nil
}
