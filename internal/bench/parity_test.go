package bench

import (
	"testing"

	"gosteal/internal/ws"
)

// TestGoParity verifies parallel Go analysis equals the serial result at
// several worker counts and under both steal policies.
func TestGoParity(t *testing.T) {
	cfg := DefaultGoConfig()
	cfg.Playouts = 6 // keep the race run short; determinism is what matters
	serial := RunGoSerial(cfg)
	for _, mode := range []ws.StealMode{ws.StealHalf, ws.StealOne} {
		for _, n := range []int{1, 2, 4, 8} {
			p := ws.NewPool(n, ws.WithStealMode(mode))
			got, err := RunGoParallel(p, cfg)
			p.Shutdown()
			if err != nil {
				t.Fatalf("mode=%v n=%d: %v", mode, n, err)
			}
			if !goResultsEqual(got, serial) {
				t.Fatalf("mode=%v n=%d: Go results diverge from serial", mode, n)
			}
		}
	}
}

// TestMergeParity verifies parallel merge sort equals the serial result.
func TestMergeParity(t *testing.T) {
	cfg := DefaultMergeConfig()
	cfg.Size = 1 << 15 // shorter for race-instrumented builds
	serial := RunMergeSerial(cfg)
	for _, mode := range []ws.StealMode{ws.StealHalf, ws.StealOne} {
		for _, n := range []int{1, 2, 4, 8} {
			p := ws.NewPool(n, ws.WithStealMode(mode))
			got, err := RunMergeParallel(p, cfg)
			p.Shutdown()
			if err != nil {
				t.Fatalf("mode=%v n=%d: %v", mode, n, err)
			}
			if !equalInt64(got, serial) {
				t.Fatalf("mode=%v n=%d: merge results diverge", mode, n)
			}
		}
	}
}

// TestMatmulParity verifies parallel matmul equals the serial result.
func TestMatmulParity(t *testing.T) {
	cfg := DefaultMatmulConfig()
	cfg.N = 128
	serial := RunMatmulSerial(cfg)
	for _, mode := range []ws.StealMode{ws.StealHalf, ws.StealOne} {
		for _, n := range []int{1, 2, 4, 8} {
			p := ws.NewPool(n, ws.WithStealMode(mode))
			got, err := RunMatmulParallel(p, cfg)
			p.Shutdown()
			if err != nil {
				t.Fatalf("mode=%v n=%d: %v", mode, n, err)
			}
			if !equalInt64(got, serial) {
				t.Fatalf("mode=%v n=%d: matmul results diverge", mode, n)
			}
		}
	}
}

// TestGoDeterminism runs the parallel Go workload twice and confirms the
// outputs are identical across independent runs.
func TestGoDeterminism(t *testing.T) {
	cfg := DefaultGoConfig()
	cfg.Playouts = 4
	p := ws.NewPool(6)
	first, err := RunGoParallel(p, cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := RunGoParallel(p, cfg)
	p.Shutdown()
	if err != nil {
		t.Fatal(err)
	}
	if !goResultsEqual(first, second) {
		t.Fatal("Go analysis is not deterministic across runs")
	}
}
