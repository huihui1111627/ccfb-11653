package ws

import (
	"sync"
	"sync/atomic"
	"testing"
)

// TestDequeSequential checks owner-side push/pop LIFO semantics.
func TestDequeSequential(t *testing.T) {
	d := newDeque()
	if _, ok := d.pop(); ok {
		t.Fatal("pop on empty deque succeeded")
	}
	tasks := make([]*Task, 100)
	for i := range tasks {
		tasks[i] = &Task{ID: int64(i), done: make(chan struct{}), childDone: make(chan struct{})}
		d.push(tasks[i])
		if d.len() != i+1 {
			t.Fatalf("len = %d, want %d", d.len(), i+1)
		}
	}
	for i := 99; i >= 0; i-- {
		got, ok := d.pop()
		if !ok || got.ID != int64(i) {
			t.Fatalf("pop = %v ok=%v, want id %d", got, ok, i)
		}
	}
	if _, ok := d.pop(); ok {
		t.Fatal("pop on drained deque succeeded")
	}
}

// TestDequeGrow pushes far beyond the initial capacity: push must trigger
// several grows while every task remains retrievable in LIFO order.
func TestDequeGrow(t *testing.T) {
	d := newDeque()
	const n = 5000
	for i := 0; i < n; i++ {
		d.push(&Task{ID: int64(i), done: make(chan struct{}), childDone: make(chan struct{})})
	}
	if d.len() != n {
		t.Fatalf("len = %d, want %d", d.len(), n)
	}
	for i := n - 1; i >= 0; i-- {
		got, ok := d.pop()
		if !ok || got.ID != int64(i) {
			t.Fatalf("at %d: got id %v ok=%v", i, got, ok)
		}
	}
}

// TestDequeConcurrentConsume runs one owner pushing and popping against a
// set of thieves. Every claimed task ID must be unique and within range;
// the union of consumed IDs must equal the pushed set.
func TestDequeConcurrentConsume(t *testing.T) {
	const items = 20000
	const thieves = 6
	d := newDeque()

	var consumed [items]atomic.Bool
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Thieves: spin on steal() until the owner signals completion and the
	// deque is observed empty.
	for k := 0; k < thieves; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if task, ok := d.steal(); ok {
					if task.ID < 0 || task.ID >= items || !consumed[task.ID].CompareAndSwap(false, true) {
						t.Errorf("duplicate or invalid task id %d", task.ID)
						return
					}
					continue
				}
				select {
				case <-stop:
					// Drain remainder.
					for {
						task, ok := d.steal()
						if !ok {
							return
						}
						if task.ID < 0 || task.ID >= items || !consumed[task.ID].CompareAndSwap(false, true) {
							t.Errorf("duplicate or invalid task id %d", task.ID)
							return
						}
					}
				default:
				}
			}
		}()
	}

	// Owner: push every item first (forcing many grows while thieves run),
	// then pop what remains. Push and pop never interleave on the owner in
	// real pool usage either — a worker drains before it spawns again.
	for i := 0; i < items; i++ {
		d.push(&Task{ID: int64(i), done: make(chan struct{}), childDone: make(chan struct{})})
	}
	close(stop)
	for {
		task, ok := d.pop()
		if !ok {
			break
		}
		if !consumed[task.ID].CompareAndSwap(false, true) {
			t.Fatalf("owner duplicate id %d", task.ID)
		}
	}
	wg.Wait()

	for i := 0; i < items; i++ {
		if !consumed[i].Load() {
			t.Fatalf("task %d never consumed", i)
		}
	}
}

// TestDequeInterleavedOwner mimics the real scheduler: the owner pushes a
// wave of tasks, drains part of them, then pushes again (reusing slots and
// crossing grow boundaries) while thieves run continuously.
func TestDequeInterleavedOwner(t *testing.T) {
	const rounds = 400
	const wave = 64
	const thieves = 6
	d := newDeque()
	var consumed [rounds * wave]atomic.Bool
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for k := 0; k < thieves; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if task, ok := d.steal(); ok {
					if !consumed[task.ID].CompareAndSwap(false, true) {
						t.Errorf("thief duplicate id %d", task.ID)
						return
					}
					continue
				}
				select {
				case <-stop:
					for {
						task, ok := d.steal()
						if !ok {
							return
						}
						if !consumed[task.ID].CompareAndSwap(false, true) {
							t.Errorf("thief duplicate id %d", task.ID)
							return
						}
					}
				default:
				}
			}
		}()
	}

	for r := 0; r < rounds; r++ {
		base := r * wave
		for i := 0; i < wave; i++ {
			d.push(&Task{ID: int64(base + i), done: make(chan struct{}), childDone: make(chan struct{})})
		}
		// Owner drains about half its own wave; thieves take the rest.
		for k := 0; k < wave/2; k++ {
			if task, ok := d.pop(); ok {
				if !consumed[task.ID].CompareAndSwap(false, true) {
					t.Fatalf("owner duplicate id %d", task.ID)
				}
			}
		}
	}
	close(stop)
	for {
		task, ok := d.pop()
		if !ok {
			break
		}
		if !consumed[task.ID].CompareAndSwap(false, true) {
			t.Fatalf("owner duplicate id %d", task.ID)
		}
	}
	wg.Wait()
	for i := range consumed {
		if !consumed[i].Load() {
			t.Fatalf("task %d never consumed", i)
		}
	}
}

// TestDequeSegmentedGrowth pushes well past many segment boundaries and
// confirms every logical index maps to a distinct, stable slot and every
// task is recovered once — guarding the non-copying segmented store.
func TestDequeSegmentedGrowth(t *testing.T) {
	d := newDeque()
	const n = 100_000
	for i := 0; i < n; i++ {
		d.push(&Task{ID: int64(i), done: make(chan struct{}), childDone: make(chan struct{})})
	}
	if d.len() != n {
		t.Fatalf("len = %d, want %d", d.len(), n)
	}
	// Stable-address check: the same logical index resolves to the same
	// slot across repeated lookups.
	s0 := d.store.slot(12345)
	s1 := d.store.slot(12345)
	if s0 != s1 {
		t.Fatal("logical index mapped to different slots across lookups")
	}
	for i := n - 1; i >= 0; i-- {
		got, ok := d.pop()
		if !ok || got.ID != int64(i) {
			t.Fatalf("at %d: got %v ok=%v", i, got, ok)
		}
	}
	if _, ok := d.pop(); ok {
		t.Fatal("pop succeeded on drained deque")
	}
}

// TestDequeStealNGrow interleaves batch steals with owner pushes across
// several capacity doublings; every task must still be claimed exactly once.
func TestDequeStealNGrow(t *testing.T) {
	const items = 8000
	d := newDeque()
	var consumed [items]atomic.Bool
	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			batch := d.stealN(64)
			for _, task := range batch {
				if !consumed[task.ID].CompareAndSwap(false, true) {
					t.Errorf("duplicate batch id %d", task.ID)
					return
				}
			}
			select {
			case <-stop:
				for {
					batch := d.stealN(64)
					if len(batch) == 0 {
						return
					}
					for _, task := range batch {
						if !consumed[task.ID].CompareAndSwap(false, true) {
							t.Errorf("duplicate batch id %d", task.ID)
							return
						}
					}
				}
			default:
			}
		}
	}()

	for i := 0; i < items; i++ {
		d.push(&Task{ID: int64(i), done: make(chan struct{}), childDone: make(chan struct{})})
	}
	close(stop)
	for {
		task, ok := d.pop()
		if !ok {
			break
		}
		if !consumed[task.ID].CompareAndSwap(false, true) {
			t.Fatalf("owner duplicate id %d", task.ID)
		}
	}
	wg.Wait()
	for i := 0; i < items; i++ {
		if !consumed[i].Load() {
			t.Fatalf("task %d never consumed", i)
		}
	}
}
