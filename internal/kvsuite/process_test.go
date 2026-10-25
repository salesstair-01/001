package kvsuite

import (
	"flag"
	"os"
	"testing"
)

var (
	metaSeed = flag.Int64("meta-seed", 1, "first seed of the metamorphic test")
	metaRuns = flag.Int("meta-runs", 4, "seeds the metamorphic test runs")
	metaOps  = flag.Int("meta-ops", 300, "operations per seed")
)

// The suites against a kvd binary named by KVSUITE_KVD (cmd/kvd or
// kvd-rs), over gRPC, with the object store from MINIO_*.
func processTarget(t *testing.T) Target {
	bin := os.Getenv("KVSUITE_KVD")
	if bin == "" {
		t.Skip("KVSUITE_KVD is not set")
	}
	return ProcessTarget(bin)
}

func TestSemantics(t *testing.T) {
	Semantics(t, processTarget(t))
}

func TestMetamorphic(t *testing.T) {
	Metamorphic(t, *metaSeed, *metaRuns, *metaOps, map[string]Target{"kvd": processTarget(t)})
}
