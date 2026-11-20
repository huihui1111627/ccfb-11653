package ws

import (
	"sync"
	"sync/atomic"
)

// MaxStealEvents bounds the in-memory ring of steal records.
const MaxStealEvents = 300

// MaxTaskNodes bounds the retained task tree (oldest finished leaves are
// pruned first so active tasks remain visible).
const MaxTaskNodes = 5000

// StealEvent is one recorded stealing attempt outcome.
type StealEvent struct {
	Seq       int64 `json:"seq"`
	Victim    int   `json:"victim"`
	Thief     int   `json:"thief"`
	Count     int   `json:"count"`
	TaskID    int64 `json:"taskId"`
	Timestamp int64 `json:"tsNanos"` // wall-clock nanos from time.Now
}

// TaskNode is the monitor's record of one task.
type TaskNode struct {
	ID        int64   `json:"id"`
	ParentID  int64   `json:"parentId"`
	Name      string  `json:"name"`
	Depth     int     `json:"depth"`
	State     string  `json:"state"`
	Worker    int     `json:"worker"` // worker executing/executing it, -1 if never started
	Stolen    bool    `json:"stolen"` // executed by a different worker than its enqueuer
	EnqWorker int     `json:"enqWorker"`
	ChildIDs  []int64 `json:"childIds"`
}

// Monitor collects everything the visualization page renders. It is safe
// for concurrent use; a single mutex guards the registry since monitor
// operations sit off the scheduling hot path (the deques remain lock-free).
type Monitor struct {
	mu      sync.Mutex
	nextID  int64
	nodes   map[int64]*TaskNode
	parents map[int64]*Task
	// child counts for childDone accounting
	childTotal map[int64]int32
	childDone  map[int64]int32
	childClose map[int64]bool
	order      []int64 // insertion order, for pruning and tree rendering

	stealSeq atomic.Int64
	stealMu  sync.Mutex
	steals   []StealEvent

	pruned atomic.Int64
}

func NewMonitor() *Monitor {
	return &Monitor{
		nodes:      make(map[int64]*TaskNode),
		parents:    make(map[int64]*Task),
		childTotal: make(map[int64]int32),
		childDone:  make(map[int64]int32),
		childClose: make(map[int64]bool),
	}
}

// register creates a task and wires parent/child bookkeeping.
func (m *Monitor) register(parent *Task, name string, fn func(*TaskCtx) error) *Task {
	id := atomic.AddInt64(&m.nextID, 1)
	t := newTask(id, parent, name, fn)

	m.mu.Lock()
	m.parents[id] = t
	node := &TaskNode{
		ID:        id,
		ParentID:  -1,
		Name:      name,
		Depth:     t.depth,
		State:     "ready",
		Worker:    -1,
		EnqWorker: -1,
	}
	if parent != nil {
		node.ParentID = parent.ID
		pn := m.nodes[parent.ID]
		if pn != nil {
			pn.ChildIDs = append(pn.ChildIDs, id)
		}
		m.childTotal[parent.ID]++
	}
	m.nodes[id] = node
	m.order = append(m.order, id)
	m.pruneLocked()
	m.mu.Unlock()
	return t
}

func (m *Monitor) parent(t *Task) *Task {
	if t.ParentID < 0 {
		return nil
	}
	m.mu.Lock()
	p := m.parents[t.ParentID]
	m.mu.Unlock()
	return p
}

func (m *Monitor) noteEnqueue(worker int, taskID int64) {
	m.mu.Lock()
	if n := m.nodes[taskID]; n != nil {
		n.EnqWorker = worker
	}
	m.mu.Unlock()
}

func (m *Monitor) noteStart(taskID int64, worker int, stolen bool) {
	m.mu.Lock()
	if n := m.nodes[taskID]; n != nil {
		n.State = "running"
		n.Worker = worker
		n.Stolen = stolen
	}
	m.mu.Unlock()
}

func (m *Monitor) noteFinish(taskID int64, worker int) {
	m.mu.Lock()
	if n := m.nodes[taskID]; n != nil {
		n.State = "done"
		n.Worker = worker
	}
	m.mu.Unlock()
}

func (m *Monitor) noteSteal(victim, thief, count int, taskID int64, nowNanos int64) {
	ev := StealEvent{
		Seq:       m.stealSeq.Add(1),
		Victim:    victim,
		Thief:     thief,
		Count:     count,
		TaskID:    taskID,
		Timestamp: nowNanos,
	}
	m.stealMu.Lock()
	m.steals = append(m.steals, ev)
	if len(m.steals) > MaxStealEvents {
		m.steals = m.steals[len(m.steals)-MaxStealEvents:]
	}
	m.stealMu.Unlock()
}

// childFinished records that one direct child of parent has fully completed.
// It closes parent.childDone exactly once, after the last child.
func (m *Monitor) childFinished(parent *Task) {
	m.mu.Lock()
	done := m.childDone[parent.ID] + 1
	m.childDone[parent.ID] = done
	total := m.childTotal[parent.ID]
	shouldClose := done >= total && !m.childClose[parent.ID]
	if shouldClose {
		m.childClose[parent.ID] = true
	}
	m.mu.Unlock()
	if shouldClose {
		close(parent.childDone)
	}
}

// pruneLocked drops finished leaf tasks beyond MaxTaskNodes, oldest first.
// It makes at most one pass over the insertion-order list: any node that is
// still active or keeps children is retained, so pruning stops as soon as
// every remaining candidate is protected — never loops on a protected head.
func (m *Monitor) pruneLocked() {
	if len(m.order) <= MaxTaskNodes {
		return
	}
	// Allocate a fresh slice: reusing m.order's backing array while ranging
	// over it would overwrite elements still being visited and corrupt the
	// parent/child map lookups.
	kept := make([]int64, 0, MaxTaskNodes)
	removed := 0
	for _, id := range m.order {
		n := m.nodes[id]
		if n != nil && (n.State != "done" || len(n.ChildIDs) > 0) {
			kept = append(kept, id)
			continue
		}
		delete(m.nodes, id)
		delete(m.parents, id)
		removed++
	}
	m.order = kept
	m.pruned.Add(int64(removed))
}

// Snapshot returns a point-in-time copy of the monitor state for the web API.
type Snapshot struct {
	Tasks       []TaskNode   `json:"tasks"`
	Steals      []StealEvent `json:"steals"`
	PrunedTasks int64        `json:"prunedTasks"`
	TotalSteals int64        `json:"totalSteals"`
}

func (m *Monitor) Snapshot() Snapshot {
	m.mu.Lock()
	tasks := make([]TaskNode, 0, len(m.order))
	for _, id := range m.order {
		if n, ok := m.nodes[id]; ok {
			cp := *n
			cp.ChildIDs = append([]int64(nil), n.ChildIDs...)
			tasks = append(tasks, cp)
		}
	}
	pruned := m.pruned.Load()
	m.mu.Unlock()

	m.stealMu.Lock()
	steals := append([]StealEvent(nil), m.steals...)
	total := m.stealSeq.Load()
	m.stealMu.Unlock()

	return Snapshot{Tasks: tasks, Steals: steals, PrunedTasks: pruned, TotalSteals: total}
}

// Reset clears all monitoring state for a fresh run.
func (m *Monitor) Reset() {
	m.mu.Lock()
	m.nodes = make(map[int64]*TaskNode)
	m.parents = make(map[int64]*Task)
	m.childTotal = make(map[int64]int32)
	m.childDone = make(map[int64]int32)
	m.childClose = make(map[int64]bool)
	m.order = nil
	m.pruned.Store(0)
	m.mu.Unlock()
	m.stealMu.Lock()
	m.steals = nil
	m.stealSeq.Store(0)
	m.stealMu.Unlock()
}
