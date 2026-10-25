package kv

// GCVersions runs MVCC GC now, for the shared suites (internal/kvsuite).
func GCVersions(p *Partition) (int, error) { return p.gcVersions() }
