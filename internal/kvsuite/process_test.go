package kvsuite

import (
	"flag"
	"os"
	"testing"

	"github.com/salesstair-01/001/internal/kvtest"
)

var (
	metaSeed = flag.Int64("meta-seed", 1, "first seed of the metamorphic test")
	metaRuns = flag.Int("meta-runs", 4, "seeds the metamorphic test runs")
	metaOps  = flag.Int("meta-ops", 300, "operations per seed")
)

// The suites against kvd over gRPC: the binary KVSUITE_KVD, or kvd-rs; on
// the object store from MINIO_* if MINIO_ENDPOINT is set, else on a local
// store.
func processTarget(t *testing.T) Target {
	bin := os.Getenv("KVSUITE_KVD")
	if bin == "" {
		bin = kvtest.Binary(t)
	}
	store := ""
	if os.Getenv("MINIO_ENDPOINT") == "" {
		store = t.TempDir()
	}
	return ProcessTarget(bin, store)
}

func TestSemantics(t *testing.T) {
	Semantics(t, processTarget(t))
}

func TestMetamorphic(t *testing.T) {
	Metamorphic(t, *metaSeed, *metaRuns, *metaOps, map[string]Target{"kvd": processTarget(t)})
}
