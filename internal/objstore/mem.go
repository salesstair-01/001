package objstore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Mem is an in-memory Store with the same put-if-absent semantics as MinIO.
// Tests use it, often wrapped in Faulty, so seeded fault runs are fast.
type Mem struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func NewMem() *Mem { return &Mem{objs: map[string][]byte{}} }

func (m *Mem) PutIfAbsent(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objs[key]; ok {
		return ErrPreconditionFailed
	}
	m.objs[key] = append([]byte(nil), data...)
	return nil
}

func (m *Mem) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), b...), nil
}

func (m *Mem) List(_ context.Context, prefix, startAfter string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for k := range m.objs {
		if strings.HasPrefix(k, prefix) && k > startAfter {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (m *Mem) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, key)
	return nil
}

func (m *Mem) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, ErrNotFound
	}
	if off < 0 || n < 0 || off+n > int64(len(b)) {
		return nil, fmt.Errorf("objstore: range %d+%d past the end of %s (%d bytes)", off, n, key, len(b))
	}
	return append([]byte(nil), b[off:off+n]...), nil
}

func (m *Mem) Size(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return 0, ErrNotFound
	}
	return int64(len(b)), nil
}

// Keys returns every stored key (for tests).
func (m *Mem) Keys() []string {
	keys, _ := m.List(context.Background(), "", "")
	return keys
}
