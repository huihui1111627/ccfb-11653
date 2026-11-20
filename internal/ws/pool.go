package ws

import (
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// StealMode selects how many tasks a thief takes from a victim.
type StealMode int

const (
	// StealOne takes exactly one task per successful steal.
	StealOne StealMode = iota
	// StealHalf takes half of the victim's deque (the standard
	// work-stealing policy).
	StealHalf
)

// String implements fmt.Stringer.
func (m StealMode) String() string {
	if m == StealHalf {
		return "half"
	}
	return "one"
}

// Worker states, used for the busy/steal/idle time accounting.
type WorkerState uint8

const (
	StateIdle  WorkerState = iota // parked, nothing to do
	StateBusy                     // executing a task
	StateSteal                    // looking for work (scan + backoff)
)

func (s WorkerState) String() string {
	switch s {
	case StateBusy:
		return "busy"
	case StateSteal:
		return "stealing"
	default:
		return "idle"
	}
}

// WorkerStats is a point-in-time view of one worker's accounting.
type WorkerStats struct {
	ID            int         `json:"id"`
	State         WorkerState `json:"state"`
	QueueLen      int         `json:"queueLen"`
	TasksExecuted int64       `json:"tasksExecuted"`
	TasksStolen   int64       `json:"tasksStolen"`
	TimesStolen   int64       `json:"timesStolen"` // this worker's deque robbed
	BusyNanos     int64       `json:"busyNanos"`
	StealNanos    int64       `json:"stealNanos"`
	IdleNanos     int64       `json:"idleNanos"`
	Spins         int64       `json:"spins"`
	Parks         int64       `json:"parks"`
}

// Pool is the work-stealing runtime: a fixed set of worker threads, each
// owning a Chase-Lev deque. Go's goroutine scheduler is only used to map
// these goroutines onto OS threads (LockOSThread); all task scheduling,
// queuing and load balancing is handled here.
type Pool struct {
	n       int
	workers []*Worker
	deques  []*deque

	// inflight counts tasks whose done channel has not yet closed.
	inflight atomic.Int64
	stopped  atomic.Bool

	// wake is a buffered semaphore used to park and wake idle workers.
	// Spawn and task-completion post one token per event; the buffer is
	// sized generously so a token is never dropped while a worker is
	// actually asleep, and every park additionally times out as a safety
	// net against a lost wake-up.
	wake chan struct{}

	stealMode atomic.Uint32
	monitor   *Monitor

	// GOMAXPROCS bookkeeping so Run restores the host process setting.
	prevMaxProcs atomic.Int32
}

// Worker is one worker thread's mutable state.
type Worker struct {
	id    int
	pool  *Pool
	rng   *rand.Rand
	deque *deque
	timer *time.Timer

	// State accounting. Counters are atomic so the monitoring page can
	// read them concurrently without a data race; time accumulators are
	// written only by the owning worker but read by snapshot goroutines.
	state      atomic.Uint32
	stateSince atomic.Int64 // unixNanos

	executed atomic.Int64
	stolen   atomic.Int64
	robbed   atomic.Int64
	busyNs   atomic.Int64
	stealNs  atomic.Int64
	idleNs   atomic.Int64
	spins    atomic.Int64
	parks    atomic.Int64
}

// Option configures a Pool at construction.
type Option func(*Pool)

// WithStealMode sets the initial stealing policy.
func WithStealMode(m StealMode) Option {
	return func(p *Pool) { p.stealMode.Store(uint32(m)) }
}

// WithMonitor attaches a monitor for the visualization page.
func WithMonitor(m *Monitor) Option {
	return func(p *Pool) { p.monitor = m }
}

// NewPool creates a runtime with n worker threads and starts them.
func NewPool(n int, opts ...Option) *Pool {
	if n < 1 {
		n = 1
	}
	p := &Pool{
		n:       n,
		workers: make([]*Worker, n),
		deques:  make([]*deque, n),
		wake:    make(chan struct{}, 1024),
	}
	p.stealMode.Store(uint32(StealHalf))
	for _, o := range opts {
		o(p)
	}
	if p.monitor == nil {
		p.monitor = NewMonitor()
	}
	for i := 0; i < n; i++ {
		p.deques[i] = newDeque()
		timer := time.NewTimer(time.Hour)
		if !timer.Stop() {
			<-timer.C
		}
		w := &Worker{
			id:    i,
			pool:  p,
			rng:   rand.New(rand.NewSource(2654435761 * int64(i+17))),
			deque: p.deques[i],
			timer: timer,
		}
		p.workers[i] = w
	}
	// Start the worker threads.
	var ready sync.WaitGroup
	ready.Add(n)
	for i := 0; i < n; i++ {
		w := p.workers[i]
		go w.threadMain(&ready)
	}
	ready.Wait()
	return p
}

// Size returns the number of worker threads.
func (p *Pool) Size() int { return p.n }

// SetStealMode changes the stealing policy at runtime.
func (p *Pool) SetStealMode(m StealMode) { p.stealMode.Store(uint32(m)) }

// StealMode reports the active stealing policy.
func (p *Pool) StealMode() StealMode { return StealMode(p.stealMode.Load()) }

// Monitor returns the attached monitor.
func (p *Pool) Monitor() *Monitor { return p.monitor }

// Shutdown stops the worker threads. In-flight work must have completed
// first (Run guarantees this); calling Shutdown with tasks still reachable
// abandons those tasks.
func (p *Pool) Shutdown() {
	p.stopped.Store(true)
	// Wake every parked worker; those without a token return on their
	// 2ms timeout and observe stopped in the thread loop.
	for i := 0; i < p.n; i++ {
		p.postWake()
	}
}

// Run executes fn inside the pool and blocks until it and everything it
// spawned has completed. The returned error is the root task's error chain.
func (p *Pool) Run(name string, fn func(*TaskCtx) error) error {
	p.prevMaxProcs.Store(int32(runtime.GOMAXPROCS(p.n)))
	defer runtime.GOMAXPROCS(int(p.prevMaxProcs.Load()))

	root := p.spawn(nil, nil, name, fn)
	<-root.done
	return root.Err()
}

// Stats returns per-worker statistics snapshots.
func (p *Pool) Stats() []WorkerStats {
	out := make([]WorkerStats, p.n)
	for i, w := range p.workers {
		out[i] = w.Snapshot()
	}
	return out
}

// spawn registers a task, publishes it on worker w's deque (or deque 0 when
// called from a non-worker goroutine such as Run's caller), and wakes an
// idle worker.
func (p *Pool) spawn(w *Worker, parent *Task, name string, fn func(*TaskCtx) error) *Task {
	t := p.monitor.register(parent, name, fn)
	if parent != nil {
		parent.pending.Add(1)
	}
	p.inflight.Add(1)

	owner := w
	if owner == nil {
		owner = p.workers[0]
	}
	owner.deque.push(t)
	p.monitor.noteEnqueue(owner.id, t.ID)
	p.postWake()
	return t
}

// postWake posts one semaphore token. Non-blocking: if the buffer is full
// there are already enough pending tokens to wake every sleeping worker, so
// dropping the extra cannot strand a parked worker.
func (p *Pool) postWake() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// threadMain is the per-worker loop. It never exits except on Shutdown, so
// a single Pool can serve many Run calls. The workers are ordinary
// goroutines: the runtime owns goroutine-to-thread placement, while all task
// scheduling, queuing and load balancing are handled by this package.
func (w *Worker) threadMain(ready *sync.WaitGroup) {
	w.setState(StateIdle)
	ready.Done()

	for {
		if w.pool.stopped.Load() && w.pool.inflight.Load() == 0 {
			return
		}
		if !w.runOneCycle() {
			// No work found anywhere: park until a wake token arrives,
			// a timeout elapses (safety against lost wake-ups), or the
			// pool shuts down.
			w.setState(StateIdle)
			w.parkSleep()
		}
	}
}

// runOneCycle tries local pop, then stealing. It executes at most one task
// (or one stolen batch) and returns whether work was done.
func (w *Worker) runOneCycle() bool {
	if t, ok := w.deque.pop(); ok {
		w.setState(StateBusy)
		w.execute(t, false)
		return true
	}
	return w.trySteal()
}

// executeNextOrSteal is the help-forward entry point used by Wait: run a
// local task, or steal one, returning false when nothing is reachable.
func (w *Worker) executeNextOrSteal(p *Pool) bool {
	if t, ok := w.deque.pop(); ok {
		w.setState(StateBusy)
		w.execute(t, false)
		return true
	}
	return w.trySteal()
}

// trySteal implements random-victim selection, the configured batch size,
// and progressive backoff across consecutive failures. Backoff stays in
// user space (spins and Gosched): blocking sleeps are the job of the outer
// run loop's park, so a worker here is genuinely hunting for work.
func (w *Worker) trySteal() bool {
	w.setState(StateSteal)
	start := time.Now()
	const maxFails = 32
	fails := 0
	for fails < maxFails {
		if w.pool.stopped.Load() {
			w.stealNs.Add(time.Since(start).Nanoseconds())
			return false
		}
		victim := w.pickVictim()
		vDeque := w.pool.deques[victim]
		batch := 1
		if w.pool.StealMode() == StealHalf {
			if n := vDeque.len(); n > 1 {
				batch = n / 2
				if batch < 1 {
					batch = 1
				}
			}
		}
		tasks := vDeque.stealN(batch)
		if len(tasks) > 0 {
			w.stealNs.Add(time.Since(start).Nanoseconds())
			w.stolen.Add(int64(len(tasks)))
			w.pool.workers[victim].robbed.Add(1)
			w.pool.monitor.noteSteal(victim, w.id, len(tasks), tasks[0].ID, time.Now().UnixNano())
			// Run the first stolen task immediately; push the rest onto
			// our own deque so they become stealable in turn.
			first := tasks[0]
			for _, extra := range tasks[1:] {
				w.deque.push(extra)
			}
			w.setState(StateBusy)
			w.execute(first, true)
			return true
		}
		fails++
		w.spins.Add(1)
		// Progressive, purely in-user-space backoff.
		switch {
		case fails <= 8:
			procyield(16)
		default:
			runtime.Gosched()
		}
	}
	w.stealNs.Add(time.Since(start).Nanoseconds())
	return false
}

// pickVictim returns a uniformly random other worker index.
func (w *Worker) pickVictim() int {
	if w.pool.n == 1 {
		return 0
	}
	j := w.rng.Intn(w.pool.n - 1)
	if j >= w.id {
		j++
	}
	return j
}

// execute runs one task to completion and runs its completion protocol.
func (w *Worker) execute(t *Task, stolen bool) {
	w.executed.Add(1)
	t.state.Store(uint32(TaskRunning))
	w.pool.monitor.noteStart(t.ID, w.id, stolen)

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = &PanicError{TaskID: t.ID, Recovered: r}
			}
		}()
		err = t.fn(&TaskCtx{pool: w.pool, worker: w, task: t})
	}()
	if err != nil {
		// Record on the task and each ancestor so Wait() sees the error
		// regardless of which level is awaited.
		for anc := t; anc != nil; anc = w.pool.monitor.parent(anc) {
			anc.recordError(err)
		}
	}
	w.pool.monitor.noteFinish(t.ID, w.id)
	t.state.Store(uint32(TaskDone))
	w.complete(t)
}

// complete runs the pending-count protocol.
//
// Spawning a child adds 1 to the child's and every ancestor's pending
// counter; finishing a task subtracts 1 from its parent's and its own
// counter. A task's done channel closes when its own pending reaches zero
// (its fn returned and its whole subtree is finished); childDone closes
// when all direct children reached done.
func (w *Worker) complete(t *Task) {
	parent := w.pool.monitor.parent(t)
	if parent != nil {
		parent.pending.Add(-1)
	}
	if t.pending.Add(-1) == 0 {
		close(t.done)
		if parent != nil {
			w.pool.monitor.childFinished(parent)
		}
		// A completion can unblock a parked waiter (its Wait target just
		// closed), and that waiter's help-forward work may be the only way
		// the remaining tasks get scheduled. Post one wake token per
		// completion so the pool can never sleep with work reachable.
		w.pool.postWake()
		if w.pool.inflight.Add(-1) == 0 {
			// Last task in the pool: release every parked worker so a lost
			// wake token can never stall a Run() call.
			for i := 0; i < w.pool.n; i++ {
				w.pool.postWake()
			}
		}
		return
	}
	// t has unfinished descendants. The last descendant's complete() walks
	// up and performs t's final decrement, close and inflight accounting.
}

// --- state accounting -----------------------------------------------------

func (w *Worker) setState(s WorkerState) {
	now := time.Now()
	old := WorkerState(w.state.Swap(uint32(s)))
	if old == s {
		return
	}
	prev := time.Unix(0, w.stateSince.Swap(now.UnixNano()))
	d := now.Sub(prev).Nanoseconds()
	switch old {
	case StateBusy:
		w.busyNs.Add(d)
	case StateSteal:
		w.stealNs.Add(d)
	case StateIdle:
		w.idleNs.Add(d)
	}
}

// Snapshot returns a point-in-time view of this worker's counters.
func (w *Worker) Snapshot() WorkerStats {
	st := WorkerState(w.state.Load())
	busy, steal, idle := w.busyNs.Load(), w.stealNs.Load(), w.idleNs.Load()
	since := w.stateSince.Load()
	if since != 0 {
		d := time.Now().UnixNano() - since
		switch st {
		case StateBusy:
			busy += d
		case StateSteal:
			steal += d
		case StateIdle:
			idle += d
		}
	}
	return WorkerStats{
		ID:            w.id,
		State:         st,
		QueueLen:      w.deque.len(),
		TasksExecuted: w.executed.Load(),
		TasksStolen:   w.stolen.Load(),
		TimesStolen:   w.robbed.Load(),
		BusyNanos:     busy,
		StealNanos:    steal,
		IdleNanos:     idle,
		Spins:         w.spins.Load(),
		Parks:         w.parks.Load(),
	}
}

// --- parking --------------------------------------------------------------
//
// Workers sleep on the pool's buffered semaphore. Both spawn and task
// completion post tokens, so whenever a parked waiter could make progress a
// token is in flight. Every wait also carries a short timeout as a safety
// net against a dropped token; normal wake-ups are event-driven and wake on
// the token immediately.

func (w *Worker) parkSleep() {
	w.parks.Add(1)
	w.waitWithTimeout(2 * time.Millisecond)
}

// waitWithTimeout blocks until a wake token arrives or the timer fires.
// The timer is always fully stopped and drained on return, so the worker's
// reusable timer can never carry a stale firing into the next wait.
func (w *Worker) waitWithTimeout(d time.Duration) {
	w.timer.Reset(d)
	select {
	case <-w.pool.wake:
	case <-w.timer.C:
	}
	if !w.timer.Stop() {
		// Timer already fired: drain its buffered value if present.
		select {
		case <-w.timer.C:
		default:
		}
	}
}

// park blocks the current worker from inside Wait until stop closes (returns
// false) or a token/timeout arrives (returns true so the caller retries its
// help loop).
func (p *Pool) park(w *Worker, stop <-chan struct{}) bool {
	w.parks.Add(1)
	w.timer.Reset(2 * time.Millisecond)
	var woke bool
	select {
	case <-stop:
	case <-p.wake:
		woke = true
	case <-w.timer.C:
		woke = true
	}
	if !w.timer.Stop() {
		select {
		case <-w.timer.C:
		default:
		}
	}
	if !woke {
		return false
	}
	return true
}

// anyWork reports whether any deque currently holds a task.
func (w *Worker) anyWork() bool {
	for _, d := range w.pool.deques {
		if d.len() > 0 {
			return true
		}
	}
	return false
}

// PanicError wraps a task panic so it can be reported through the error
// chain instead of crashing the worker thread.
type PanicError struct {
	TaskID    int64 `json:"taskId"`
	Recovered any   `json:"recovered"`
}

func (e *PanicError) Error() string {
	return "task panic"
}
