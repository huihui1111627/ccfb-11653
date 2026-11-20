package bench

import (
	"fmt"
	"math/rand"
	"sort"

	"gosteal/internal/ws"
)

// MergeConfig sizes the parallel merge-sort workload.
type MergeConfig struct {
	Size  int   // element count
	Grain int   // leaf threshold below which a segment is sorted in place
	Seed  int64 // deterministic input generation
}

func DefaultMergeConfig() MergeConfig {
	return MergeConfig{Size: 1 << 18, Grain: 1 << 12, Seed: 0xC0FFEE}
}

// genInts produces the same input slice for every run.
func genInts(n int, seed int64) []int64 {
	rng := rand.New(rand.NewSource(seed))
	out := make([]int64, n)
	for i := range out {
		out[i] = rng.Int63()
	}
	return out
}

// mergeSortSerial sorts data into out over [lo, hi), using buf as scratch.
func mergeSortSerial(data, buf []int64, lo, hi int) {
	if hi-lo <= 1 {
		return
	}
	mid := lo + (hi-lo)/2
	mergeSortSerial(data, buf, lo, mid)
	mergeSortSerial(data, buf, mid, hi)
	merge(data, lo, mid, hi, buf)
	copy(data[lo:hi], buf[lo:hi])
}

func merge(data []int64, lo, mid, hi int, buf []int64) {
	i, j, k := lo, mid, lo
	for i < mid && j < hi {
		if data[i] <= data[j] {
			buf[k] = data[i]
			i++
		} else {
			buf[k] = data[j]
			j++
		}
		k++
	}
	for i < mid {
		buf[k] = data[i]
		i++
		k++
	}
	for j < hi {
		buf[k] = data[j]
		j++
		k++
	}
}

// RunMergeSerial returns a sorted copy of the generated input.
func RunMergeSerial(cfg MergeConfig) []int64 {
	data := genInts(cfg.Size, cfg.Seed)
	buf := make([]int64, len(data))
	mergeSortSerial(data, buf, 0, len(data))
	return data
}

// RunMergeParallel sorts a fresh copy of the input using recursive task
// bisection. Each task owns a disjoint segment; the two halves are sorted
// concurrently (one spawned sibling, one worked inline by the parent) and
// merged after Wait, so no two tasks touch the same indices.
func RunMergeParallel(pool *ws.Pool, cfg MergeConfig) ([]int64, error) {
	data := genInts(cfg.Size, cfg.Seed)
	buf := make([]int64, len(data))
	err := pool.Run(fmt.Sprintf("mergesort(%d)", cfg.Size), func(ctx *ws.TaskCtx) error {
		return parallelMerge(ctx, data, buf, 0, len(data), cfg.Grain)
	})
	return data, err
}

func parallelMerge(ctx *ws.TaskCtx, data, buf []int64, lo, hi, grain int) error {
	if hi-lo <= grain {
		// Leaf: serial sort of this segment.
		sub := data[lo:hi]
		sort.Slice(sub, func(i, j int) bool { return sub[i] < sub[j] })
		return nil
	}
	mid := lo + (hi-lo)/2
	right := ctx.Spawn("merge.r", func(ctx *ws.TaskCtx) error {
		return parallelMerge(ctx, data, buf, mid, hi, grain)
	})
	if err := parallelMerge(ctx, data, buf, lo, mid, grain); err != nil {
		_ = ctx.Wait(right)
		return err
	}
	if err := ctx.Wait(right); err != nil {
		return err
	}
	// Both halves sorted and disjoint: merge through the scratch buffer.
	merge(data, lo, mid, hi, buf)
	copy(data[lo:hi], buf[lo:hi])
	return nil
}
