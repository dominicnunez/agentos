package ledger

import (
	"github.com/dominicnunez/agentos/internal/events"
	"sort"
	"time"
)

// Prefix maxima identify the latest preceding event no later than selection,
// even when timestamps are equal or move backwards. Each event is inserted only
// as replay reaches it, so events after the first binding cannot invalidate it.
type snapshotTimeIndex struct {
	times     []time.Time
	sequences []int64
}

func routingSnapshotTimes(stream []events.Event) map[string]*snapshotTimeIndex {
	indexes := make(map[string]*snapshotTimeIndex)
	for _, e := range stream {
		if indexes[e.OrganizationID] == nil {
			indexes[e.OrganizationID] = &snapshotTimeIndex{}
		}
		x := indexes[e.OrganizationID]
		x.times = append(x.times, e.CreatedAt)
	}
	for _, x := range indexes {
		sort.Slice(x.times, func(i, j int) bool { return x.times[i].Before(x.times[j]) })
		x.sequences = make([]int64, len(x.times)+1)
	}
	return indexes
}

func (x *snapshotTimeIndex) add(at time.Time, sequence int64) {
	i := sort.Search(len(x.times), func(i int) bool { return !x.times[i].Before(at) }) + 1
	for ; i < len(x.sequences); i += i & -i {
		x.sequences[i] = max(x.sequences[i], sequence)
	}
}

func (x *snapshotTimeIndex) latest(at time.Time) int64 {
	i := sort.Search(len(x.times), func(i int) bool { return x.times[i].After(at) })
	var sequence int64
	for ; i > 0; i -= i & -i {
		sequence = max(sequence, x.sequences[i])
	}
	return sequence
}
