package ws

import (
	"errors"
	"sync/atomic"
)

// TaskState tracks the lifecycle of a task for the monitoring page.
type TaskState uint8

const (
	TaskReady   TaskState = iota // reachable, not yet executed
	TaskRunning                  // fn is executing on some worker
	TaskDone                     // fn returned and all descendants finished
)

// Task is the unit of work. Identity fields are immutable after
// registration; scheduling fields follow the pool's coordination protocol.
type Task struct {
	// ID is assigned at registration and is unique within a pool.
	ID int64
	// Name is an optional human-readable label (workload-defined).
	Name string
	// ParentID is -1 for root tasks.
	ParentID int64

	fn func(ctx *TaskCtx) error

	// pending counts unfinished tasks in this task's subtree, including
	// the task itself. A new child increments both its own and every
	// ancestor's pending; each task completion decrements its ancestors.
	pending atomic.Int32

	// done is closed exactly once, when pending reaches zero.
	done chan struct{}
	// childDone is closed exactly once, when all direct children have
	// reached their own done state (i.e. their whole subtrees finished).
	childDone chan struct{}

	// state for monitoring.
	state atomic.Uint32

	// err is the first error returned by the task or any descendant.
	err atomic.Pointer[taskError]

	// depth in the spawn tree, for the task-tree view.
	depth int
}

type taskError struct{ err error }

func newTask(id int64, parent *Task, name string, fn func(*TaskCtx) error) *Task {
	t := &Task{
		ID:        id,
		Name:      name,
		ParentID:  -1,
		fn:        fn,
		done:      make(chan struct{}),
		childDone: make(chan struct{}),
		depth:     0,
	}
	if parent != nil {
		t.ParentID = parent.ID
		t.depth = parent.depth + 1
	}
	t.pending.Store(1)
	t.state.Store(uint32(TaskReady))
	return t
}

func (t *Task) State() TaskState { return TaskState(t.state.Load()) }

// Err returns the first error observed by the task or any of its
// descendants, or nil.
func (t *Task) Err() error {
	if e := t.err.Load(); e != nil {
		return e.err
	}
	return nil
}

func (t *Task) recordError(err error) {
	if err == nil {
		return
	}
	t.err.CompareAndSwap(nil, &taskError{err: err})
}

// TaskCtx is passed to every task function. A ctx belongs to the worker
// executing that task; callers must not share it between tasks or store it
// past the task's return.
type TaskCtx struct {
	pool   *Pool
	worker *Worker
	task   *Task
}

// Spawn creates a child task of the currently running task and publishes
// it on the current worker's deque (where other workers may steal it).
func (c *TaskCtx) Spawn(name string, fn func(*TaskCtx) error) *Task {
	return c.pool.spawn(c.worker, c.task, name, fn)
}

// Wait blocks until t (including all of its descendants) has completed and
// returns its first recorded error. While blocked, the current worker keeps
// executing other runnable tasks — including tasks stolen from peers —
// instead of parking the OS thread.
func (c *TaskCtx) Wait(t *Task) error {
	for {
		if c.helpUntil(t.done) {
			return t.Err()
		}
		// Nothing reachable right now; park until t finishes or work appears.
		if !c.pool.park(c.worker, t.done) {
			return t.Err()
		}
	}
}

// helpUntil executes available tasks (local pop first, then stealing) until
// stop closes or no runnable task can be found. Returns true if stop closed.
func (c *TaskCtx) helpUntil(stop <-chan struct{}) bool {
	for {
		select {
		case <-stop:
			return true
		default:
		}
		if !c.worker.executeNextOrSteal(c.pool) {
			return false
		}
	}
}

// WaitChildren waits for every task spawned by the current task (and their
// descendants) to finish, returning the first recorded error.
func (c *TaskCtx) WaitChildren() error {
	t := c.task
	for {
		if c.helpUntil(t.childDone) {
			return t.Err()
		}
		if !c.pool.park(c.worker, t.childDone) {
			return t.Err()
		}
	}
}

// WorkerID reports which worker is running the current task.
func (c *TaskCtx) WorkerID() int { return c.worker.id }

// ParallelFor runs fn over [lo, hi). The interval is recursively split in
// half until each leaf covers at most grain elements; every split spawns a
// sibling task. grain <= 0 is treated as 1.
func (c *TaskCtx) ParallelFor(lo, hi, grain int, name string, fn func(ctx *TaskCtx, lo, hi int) error) error {
	if grain < 1 {
		grain = 1
	}
	if hi <= lo {
		return nil
	}
	leaf := c.Spawn(name, func(ctx *TaskCtx) error {
		return parallelForRec(ctx, lo, hi, grain, name, fn)
	})
	return c.Wait(leaf)
}

func parallelForRec(c *TaskCtx, lo, hi, grain int, name string, fn func(*TaskCtx, int, int) error) error {
	if hi-lo <= grain {
		return fn(c, lo, hi)
	}
	mid := lo + (hi-lo)/2
	right := c.Spawn(name+".r", func(ctx *TaskCtx) error {
		return parallelForRec(ctx, mid, hi, grain, name, fn)
	})
	leftErr := parallelForRec(c, lo, mid, grain, name, fn)
	rightErr := c.Wait(right)
	return joinErrors(leftErr, rightErr)
}

// joinErrors collapses nil errors.
func joinErrors(errs ...error) error {
	var nonNil []error
	for _, e := range errs {
		if e != nil {
			nonNil = append(nonNil, e)
		}
	}
	return errors.Join(nonNil...)
}
