package kv

import (
	"sync"
	"time"

	"github.com/salesstair-01/001/internal/encoding"
)

// warmRemote runs in the background after a wake on remote SSTs. It opens
// every remote table, several at a time, so that Pebble loads each one's
// footer and index blocks into the block cache. The parallel GETs cost
// about one round trip per table per warmer, and afterwards a first read
// of a row costs one GET (its data block) instead of two or three in
// sequence. Reads are served meanwhile; warming only fills caches.
func (p *Partition) warmRemote() {
	defer close(p.warmed)
	start := time.Now()
	release, err := p.readDB()
	if err != nil {
		return
	}
	spans, err := p.db.RemoteTables()
	release()
	if err != nil || len(spans) == 0 {
		return
	}
	work := make(chan [2][]byte)
	var wg sync.WaitGroup
	for i := 0; i < p.cfg.RemoteWarmers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range work {
				p.warmSpan(s[0], s[1])
			}
		}()
	}
	n := 0
loop:
	for _, s := range spans {
		select {
		case work <- s:
			n++
		case <-p.stopCh:
			break loop
		}
	}
	close(work)
	wg.Wait()
	p.log.Debug("warmed remote tables", "tables", n, "took", time.Since(start).Round(time.Millisecond))
}

// warmSpan positions an iterator at the first key of a table's span, which
// opens the table at every level that overlaps it.
func (p *Partition) warmSpan(first, last []byte) {
	release, err := p.readDB()
	if err != nil {
		return
	}
	defer release()
	uk, _, err := encoding.SplitMVCC(last)
	if err != nil {
		return
	}
	it, err := p.db.NewIter(first, pastVersions(uk))
	if err != nil {
		return
	}
	it.First()
	_ = it.Close()
}

// RemoteWarmed is closed once the background warming after a wake is done
// (at once when the partition has no remote SSTs).
func (p *Partition) RemoteWarmed() <-chan struct{} { return p.warmed }
