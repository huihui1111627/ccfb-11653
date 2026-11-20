package ws

import "sync/atomic"

// minLogSize is the log2 of the initial segment capacity.
const minLogSize = 3

// segment is one fixed-size chunk of the deque's logical slot space. Segments
// are never relocated: a slot at logical index i keeps the same address for
// its whole life, so growing the deque only allocates NEW segments and never
// copies or moves an existing task pointer. This removes the dual-array read
// window that a copying grow creates for concurrent thieves.
type segment struct {
	slots []atomic.Pointer[Task]
}

// segmentedStore maps ever-growing logical indices to stable slots. Segment
// k covers logical indices [k*cap, (k+1)*cap). The table of segments is
// append-only and its entries are published atomically, so any goroutine that
// computes a segment index can safely load it.
type segmentedStore struct {
	logSize uint64
	table   atomic.Pointer[[]*segment]
}

func newSegmentedStore(logSize uint64) *segmentedStore {
	s := &segmentedStore{logSize: logSize}
	first := []*segment{newSegment(1 << logSize)}
	s.table.Store(&first)
	return s
}

func newSegment(capacity uint64) *segment {
	return &segment{slots: make([]atomic.Pointer[Task], capacity)}
}

func (s *segmentedStore) cap() uint64 { return uint64(1) << s.logSize }

// slot returns the stable slot for logical index i, growing the segment table
// (never the slots) if needed.
func (s *segmentedStore) slot(i uint64) *atomic.Pointer[Task] {
	segIdx := i / s.cap()
	off := i % s.cap()
	table := s.table.Load()
	if int(segIdx) < len(*table) {
		return &(*table)[segIdx].slots[off]
	}
	return s.growSlot(segIdx, off)
}

// growSlot appends segments up to segIdx. The append is done under no lock:
// concurrent growers CAS a fresh table that includes every existing segment,
// so no published slot is ever replaced.
func (s *segmentedStore) growSlot(segIdx, off uint64) *atomic.Pointer[Task] {
	for {
		old := s.table.Load()
		if int(segIdx) < len(*old) {
			return &(*old)[segIdx].slots[off]
		}
		next := make([]*segment, segIdx+1)
		copy(next, *old)
		for k := uint64(len(*old)); k <= segIdx; k++ {
			next[k] = newSegment(s.cap())
		}
		if s.table.CompareAndSwap(old, &next) {
			return &next[segIdx].slots[off]
		}
	}
}

// deque is a Chase-Lev work-stealing deque (Chase & Lev, SPAA 2009). One
// owner pushes/pops at the bottom; any number of thieves steal from the top.
// Cross-goroutine coordination is entirely atomic — no mutexes on the hot
// path — and because slots are stable across growth, there is no copying
// window: a task at logical index i is reachable at exactly one address.
type deque struct {
	top    atomic.Uint64
	bottom atomic.Uint64
	store  *segmentedStore
}

func newDeque() *deque {
	return &deque{store: newSegmentedStore(minLogSize)}
}

// len returns an approximate live-element count. Safe for any goroutine.
func (d *deque) len() int {
	b := d.bottom.Load()
	t := d.top.Load()
	if b <= t {
		return 0
	}
	return int(b - t)
}

// push adds a task at the bottom. Owner only.
func (d *deque) push(t *Task) {
	b := d.bottom.Load()
	d.store.slot(b).Store(t)
	// Publish the element before advancing bottom.
	d.bottom.Store(b + 1)
}

// pop removes and returns the task at the bottom. Owner only.
func (d *deque) pop() (*Task, bool) {
	b := d.bottom.Load()
	tv := d.top.Load()
	if b <= tv {
		// Empty: do not decrement, the unsigned index must not underflow.
		return nil, false
	}
	b--
	// Fence thieves out of every slot except possibly the new bottom.
	d.bottom.Store(b)
	tv = d.top.Load()

	switch {
	case b < tv:
		// A thief emptied the deque while bottom was being lowered.
		d.bottom.Store(tv)
		return nil, false

	case b > tv:
		// More than one element: thieves only touch top, this is private.
		slot := d.store.slot(b)
		t := slot.Load()
		slot.Store(nil)
		return t, true

	default:
		// Exactly one element: race the thieves on top. The winner takes it.
		slot := d.store.slot(b)
		t := slot.Load()
		if !d.top.CompareAndSwap(tv, tv+1) {
			// A thief won; the deque is empty at tv+1.
			d.bottom.Store(tv + 1)
			return nil, false
		}
		d.bottom.Store(tv + 1)
		slot.Store(nil)
		return t, true
	}
}

// steal removes one task from the top. Any non-owner may call this.
func (d *deque) steal() (*Task, bool) {
	tv := d.top.Load()
	b := d.bottom.Load()
	if tv >= b {
		return nil, false
	}
	slot := d.store.slot(tv)
	t := slot.Load()
	if t == nil {
		// Owner is mid-push: the slot is not published yet. Retry later.
		return nil, false
	}
	// One decision point shared with the owner's last-element pop and with
	// every other thief: only the winning CAS may take this slot.
	if !d.top.CompareAndSwap(tv, tv+1) {
		return nil, false
	}
	slot.Store(nil)
	return t, true
}

// stealN claims up to max tasks from the top. Each claim runs the full
// steal() protocol against a stable slot, so every element is handed over
// exactly once and a concurrent owner pop or another stealer simply ends the
// batch early.
func (d *deque) stealN(max int) []*Task {
	out := make([]*Task, 0, max)
	for len(out) < max {
		t, ok := d.steal()
		if !ok {
			break
		}
		out = append(out, t)
	}
	return out
}
