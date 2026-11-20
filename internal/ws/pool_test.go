package ws

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestPoolFib computes fib(20) through spawned recursive tasks and checks
// the result against the known value.
func TestPoolFib(t *testing.T) {
	p := NewPool(4)
	defer p.Shutdown()

	var fib func(ctx *TaskCtx, n int) (int, error)
	fib = func(ctx *TaskCtx, n int) (int, error) {
		if n < 2 {
			return n, nil
		}
		var right int
		tr := ctx.Spawn(fmt.Sprintf("fib-%d", n-2), func(ctx *TaskCtx) error {
			v, err := fib(ctx, n-2)
			if err != nil {
				return err
			}
			right = v
			return nil
		})
		left, err := fib(ctx, n-1)
		if err != nil {
			return 0, err
		}
		if err := ctx.Wait(tr); err != nil {
			return 0, err
		}
		return left + right, nil
	}

	var got int
	err := p.Run("fib20", func(ctx *TaskCtx) error {
		v, e := fib(ctx, 20)
		got = v
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != 6765 {
		t.Fatalf("fib(20) = %d, want 6765", got)
	}
}

// TestParallelForSums checks the recursive-bisection parallel loop over a
// range whose size is not a power of two, with several grain settings.
func TestParallelForSums(t *testing.T) {
	p := NewPool(8)
	defer p.Shutdown()

	for _, grain := range []int{1, 3, 7, 100, 100000} {
		const n = 9973
		var sum atomic.Int64
		err := p.Run("parallelFor", func(ctx *TaskCtx) error {
			return ctx.ParallelFor(0, n, grain, "sum", func(ctx *TaskCtx, lo, hi int) error {
				var s int64
				for i := lo; i < hi; i++ {
					s += int64(i)
				}
				sum.Add(s)
				return nil
			})
		})
		if err != nil {
			t.Fatalf("grain %d: %v", grain, err)
		}
		var want int64
		for i := 0; i < n; i++ {
			want += int64(i)
		}
		if sum.Load() != want {
			t.Fatalf("grain %d: sum = %d, want %d", grain, sum.Load(), want)
		}
	}
}

// TestParallelForGuaranteesEachIndex ensures every index is visited once.
func TestParallelForGuaranteesEachIndex(t *testing.T) {
	p := NewPool(4)
	defer p.Shutdown()
	const n = 3001
	seen := make([]atomic.Int32, n)
	err := p.Run("cover", func(ctx *TaskCtx) error {
		return ctx.ParallelFor(0, n, 5, "cover", func(ctx *TaskCtx, lo, hi int) error {
			for i := lo; i < hi; i++ {
				seen[i].Add(1)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if seen[i].Load() != 1 {
			t.Fatalf("index %d visited %d times", i, seen[i].Load())
		}
	}
}

// TestErrorPropagation returns an error from a deep leaf and checks that
// every ancestor's Wait surfaces it, including through WaitChildren.
func TestErrorPropagation(t *testing.T) {
	p := NewPool(4)
	defer p.Shutdown()
	boom := errors.New("boom")

	err := p.Run("errRoot", func(ctx *TaskCtx) error {
		var sawOnChild error
		child := ctx.Spawn("child", func(ctx *TaskCtx) error {
			leaf := ctx.Spawn("leaf", func(ctx *TaskCtx) error {
				return boom
			})
			sawOnChild = ctx.Wait(leaf)
			return nil
		})
		if err := ctx.Wait(child); err != nil {
			return err
		}
		if !errors.Is(sawOnChild, boom) {
			return fmt.Errorf("child Wait did not see boom: %v", sawOnChild)
		}
		return ctx.WaitChildren()
	})
	if !errors.Is(err, boom) {
		t.Fatalf("root err = %v, want boom", err)
	}
}

// TestPanicBecomesError checks a panicking task does not kill its worker.
func TestPanicBecomesError(t *testing.T) {
	p := NewPool(3)
	defer p.Shutdown()
	var ranAfter atomic.Bool
	err := p.Run("panicRoot", func(ctx *TaskCtx) error {
		bad := ctx.Spawn("bad", func(ctx *TaskCtx) error {
			panic("kaboom")
		})
		good := ctx.Spawn("good", func(ctx *TaskCtx) error {
			ranAfter.Store(true)
			return nil
		})
		e1 := ctx.Wait(bad)
		e2 := ctx.Wait(good)
		if e1 == nil {
			return errors.New("panic task returned nil error")
		}
		var pe *PanicError
		if !errors.As(e1, &pe) {
			return fmt.Errorf("expected PanicError, got %T %v", e1, e1)
		}
		return e2
	})
	if err == nil {
		t.Fatal("expected error from panic run")
	}
	if !ranAfter.Load() {
		t.Fatal("sibling task did not run after panic")
	}
	// Pool must still be usable after the panic.
	var ok atomic.Bool
	if e := p.Run("after", func(ctx *TaskCtx) error { ok.Store(true); return nil }); e != nil {
		t.Fatalf("pool unusable after panic: %v", e)
	}
	if !ok.Load() {
		t.Fatal("post-panic run did not execute")
	}
}

// TestWaitHelpsForward spawns more work than workers and makes the root
// task Wait while blocked. With one worker this can only finish if the
// waiting worker executes other tasks itself.
func TestWaitHelpsForward(t *testing.T) {
	for _, n := range []int{1, 2, 4} {
		p := NewPool(n)
		const tasks = 200
		var done atomic.Int32
		err := p.Run("help", func(ctx *TaskCtx) error {
			kids := make([]*Task, tasks)
			for i := 0; i < tasks; i++ {
				kids[i] = ctx.Spawn("k", func(ctx *TaskCtx) error {
					// Small busy wait so tasks queue up and get stolen.
					x := 0
					for j := 0; j < 10000; j++ {
						x++
					}
					done.Add(1)
					_ = x
					return nil
				})
			}
			for _, k := range kids {
				if err := ctx.Wait(k); err != nil {
					return err
				}
			}
			return nil
		})
		p.Shutdown()
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if done.Load() != tasks {
			t.Fatalf("n=%d: completed %d, want %d", n, done.Load(), tasks)
		}
	}
}

// TestStress hammers the pool with many short runs to expose races and
// lost-wake-up hangs. Run under -race for the full guarantee.
func TestStress(t *testing.T) {
	p := NewPool(8)
	defer p.Shutdown()
	deadline := time.Now().Add(3 * time.Second)
	var run int
	for time.Now().Before(deadline) {
		const n = 500
		var sum atomic.Int64
		err := p.Run(fmt.Sprintf("stress-%d", run), func(ctx *TaskCtx) error {
			return ctx.ParallelFor(0, n, 3, "s", func(ctx *TaskCtx, lo, hi int) error {
				var s int64
				for i := lo; i < hi; i++ {
					s += int64(i)
				}
				sum.Add(s)
				return nil
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		var want int64
		for i := 0; i < n; i++ {
			want += int64(i)
		}
		if sum.Load() != want {
			t.Fatalf("run %d: %d != %d", run, sum.Load(), want)
		}
		run++
	}
	t.Logf("completed %d stress runs", run)
}

// TestSingleWorkerSemantics verifies correct results with no peers to steal.
func TestSingleWorkerSemantics(t *testing.T) {
	p := NewPool(1)
	defer p.Shutdown()
	var sum atomic.Int64
	err := p.Run("one", func(ctx *TaskCtx) error {
		return ctx.ParallelFor(0, 1000, 7, "x", func(ctx *TaskCtx, lo, hi int) error {
			for i := lo; i < hi; i++ {
				sum.Add(int64(i))
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	var want int64
	for i := 0; i < 1000; i++ {
		want += int64(i)
	}
	if sum.Load() != want {
		t.Fatalf("%d != %d", sum.Load(), want)
	}
}

// TestStealModesBothCorrect runs the same workload under both policies.
func TestStealModesBothCorrect(t *testing.T) {
	for _, mode := range []StealMode{StealOne, StealHalf} {
		p := NewPool(6, WithStealMode(mode))
		const n = 4096
		seen := make([]atomic.Int32, n)
		err := p.Run("modes", func(ctx *TaskCtx) error {
			return ctx.ParallelFor(0, n, 4, "m", func(ctx *TaskCtx, lo, hi int) error {
				for i := lo; i < hi; i++ {
					seen[i].Add(1)
				}
				return nil
			})
		})
		p.Shutdown()
		if err != nil {
			t.Fatalf("mode %d: %v", mode, err)
		}
		for i := 0; i < n; i++ {
			if seen[i].Load() != 1 {
				t.Fatalf("mode %d: index %d seen %d", mode, i, seen[i].Load())
			}
		}
	}
}
