package bench

import (
	"fmt"
	"math/rand"

	"gosteal/internal/ws"
)

// MatmulConfig sizes the matrix-multiplication workload (C = A * B).
type MatmulConfig struct {
	N     int // square matrix dimension
	Grain int // rows per ParallelFor leaf
	Seed  int64
}

func DefaultMatmulConfig() MatmulConfig {
	return MatmulConfig{N: 384, Grain: 16, Seed: 0xBADC0DE}
}

func genMatrix(n int, seed int64) []int64 {
	rng := rand.New(rand.NewSource(seed))
	m := make([]int64, n*n)
	for i := range m {
		// Keep values small so dot products never overflow int64.
		m[i] = rng.Int63n(1000) - 500
	}
	return m
}

// RunMatmulSerial computes C = A*B for row-major square matrices on one
// goroutine.
func RunMatmulSerial(cfg MatmulConfig) []int64 {
	n := cfg.N
	a := genMatrix(n, cfg.Seed)
	b := genMatrix(n, cfg.Seed^0x5DEECE66D)
	c := make([]int64, n*n)
	for i := 0; i < n; i++ {
		for k := 0; k < n; k++ {
			aik := a[i*n+k]
			for j := 0; j < n; j++ {
				c[i*n+j] += aik * b[k*n+j]
			}
		}
	}
	return c
}

// RunMatmulParallel computes the same product with row ranges split across
// the work-stealing pool. Each leaf owns a disjoint set of output rows, so
// workers write disjoint memory and need no locks.
func RunMatmulParallel(pool *ws.Pool, cfg MatmulConfig) ([]int64, error) {
	n := cfg.N
	a := genMatrix(n, cfg.Seed)
	b := genMatrix(n, cfg.Seed^0x5DEECE66D)
	c := make([]int64, n*n)
	err := pool.Run(fmt.Sprintf("matmul(%d)", n), func(ctx *ws.TaskCtx) error {
		return ctx.ParallelFor(0, n, cfg.Grain, "matmul-rows", func(ctx *ws.TaskCtx, lo, hi int) error {
			for i := lo; i < hi; i++ {
				for k := 0; k < n; k++ {
					aik := a[i*n+k]
					row := c[i*n : i*n+n]
					brow := b[k*n : k*n+n]
					for j := 0; j < n; j++ {
						row[j] += aik * brow[j]
					}
				}
			}
			return nil
		})
	})
	return c, err
}
