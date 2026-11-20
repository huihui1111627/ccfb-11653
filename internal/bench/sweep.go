package bench

import (
	"bytes"
	"fmt"
	"time"

	"gosteal/internal/ws"
)

// Workload identifies one of the three benchmark programs.
type Workload string

const (
	WorkloadGo     Workload = "go"
	WorkloadMerge  Workload = "merge"
	WorkloadMatmul Workload = "matmul"
)

// ThreadResult is one (worker count, steal mode) measurement.
type ThreadResult struct {
	Workers    int           `json:"workers"`
	StealMode  string        `json:"stealMode"`
	Elapsed    time.Duration `json:"elapsed"`
	ElapsedMS  float64       `json:"elapsedMs"`
	Speedup    float64       `json:"speedup"`    // vs serial reference time
	Efficiency float64       `json:"efficiency"` // speedup / workers
	Correct    bool          `json:"correct"`
	Detail     string        `json:"detail,omitempty"`
}

// Report collects a full benchmark sweep.
type Report struct {
	Workload    Workload       `json:"workload"`
	SerialTime  time.Duration  `json:"serialTime"`
	SerialMS    float64        `json:"serialMs"`
	HalfResults []ThreadResult `json:"halfResults"`
	OneResults  []ThreadResult `json:"oneResults"`
}

// equalInt64 compares two result slices exactly.
func equalInt64(a, b []int64) bool { return bytes.Equal(int64Bytes(a), int64Bytes(b)) }

func int64Bytes(s []int64) []byte {
	// Avoid unsafe; a length-prefix plus values comparison via a buffer.
	var buf bytes.Buffer
	var tmp [8]byte
	for _, v := range s {
		x := uint64(v)
		for i := 0; i < 8; i++ {
			tmp[i] = byte(x >> (8 * i))
		}
		buf.Write(tmp[:])
	}
	return buf.Bytes()
}

func goResultsEqual(a, b []GoResult) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SweepGo runs the Go workload serially then under 1..maxWorkers for both
// steal policies, verifying every parallel run equals the serial output.
func SweepGo(maxWorkers int, cfg GoConfig) (*Report, error) {
	serial := RunGoSerial(cfg)
	serialTime := timeGo(cfg)
	rep := &Report{Workload: WorkloadGo, SerialTime: serialTime, SerialMS: ms(serialTime)}

	for _, mode := range []struct {
		mode ws.StealMode
		name string
		dst  *[]ThreadResult
	}{
		{ws.StealHalf, "half", &rep.HalfResults},
		{ws.StealOne, "one", &rep.OneResults},
	} {
		for n := 1; n <= maxWorkers; n++ {
			pool := ws.NewPool(n, ws.WithStealMode(mode.mode))
			start := time.Now()
			got, err := RunGoParallel(pool, cfg)
			elapsed := time.Since(start)
			pool.Shutdown()
			r := ThreadResult{
				Workers: n, StealMode: mode.name,
				Elapsed: elapsed, ElapsedMS: ms(elapsed),
				Speedup: float64(serialTime) / float64(elapsed),
				Correct: err == nil && goResultsEqual(got, serial),
			}
			r.Efficiency = r.Speedup / float64(n)
			if err != nil {
				r.Detail = err.Error()
			}
			*mode.dst = append(*mode.dst, r)
		}
	}
	return rep, nil
}

// SweepMerge runs the merge-sort sweep.
func SweepMerge(maxWorkers int, cfg MergeConfig) (*Report, error) {
	serial := RunMergeSerial(cfg)
	serialTime := timeMerge(cfg)
	rep := &Report{Workload: WorkloadMerge, SerialTime: serialTime, SerialMS: ms(serialTime)}

	for _, mode := range []struct {
		mode ws.StealMode
		name string
		dst  *[]ThreadResult
	}{
		{ws.StealHalf, "half", &rep.HalfResults},
		{ws.StealOne, "one", &rep.OneResults},
	} {
		for n := 1; n <= maxWorkers; n++ {
			pool := ws.NewPool(n, ws.WithStealMode(mode.mode))
			start := time.Now()
			got, err := RunMergeParallel(pool, cfg)
			elapsed := time.Since(start)
			pool.Shutdown()
			r := ThreadResult{
				Workers: n, StealMode: mode.name,
				Elapsed: elapsed, ElapsedMS: ms(elapsed),
				Speedup: float64(serialTime) / float64(elapsed),
				Correct: err == nil && equalInt64(got, serial),
			}
			r.Efficiency = r.Speedup / float64(n)
			if err != nil {
				r.Detail = err.Error()
			}
			*mode.dst = append(*mode.dst, r)
		}
	}
	return rep, nil
}

// SweepMatmul runs the matrix-multiplication sweep.
func SweepMatmul(maxWorkers int, cfg MatmulConfig) (*Report, error) {
	serial := RunMatmulSerial(cfg)
	serialTime := timeMatmul(cfg)
	rep := &Report{Workload: WorkloadMatmul, SerialTime: serialTime, SerialMS: ms(serialTime)}

	for _, mode := range []struct {
		mode ws.StealMode
		name string
		dst  *[]ThreadResult
	}{
		{ws.StealHalf, "half", &rep.HalfResults},
		{ws.StealOne, "one", &rep.OneResults},
	} {
		for n := 1; n <= maxWorkers; n++ {
			pool := ws.NewPool(n, ws.WithStealMode(mode.mode))
			start := time.Now()
			got, err := RunMatmulParallel(pool, cfg)
			elapsed := time.Since(start)
			pool.Shutdown()
			r := ThreadResult{
				Workers: n, StealMode: mode.name,
				Elapsed: elapsed, ElapsedMS: ms(elapsed),
				Speedup: float64(serialTime) / float64(elapsed),
				Correct: err == nil && equalInt64(got, serial),
			}
			r.Efficiency = r.Speedup / float64(n)
			if err != nil {
				r.Detail = err.Error()
			}
			*mode.dst = append(*mode.dst, r)
		}
	}
	return rep, nil
}

// AllCorrect reports whether every measured run matched the serial output.
func (r *Report) AllCorrect() bool {
	for _, x := range append(append([]ThreadResult{}, r.HalfResults...), r.OneResults...) {
		if !x.Correct {
			return false
		}
	}
	return true
}

func (r *Report) String() string {
	var b bytes.Buffer
	fmt.Fprintf(&b, "workload=%s serial=%.2fms\n", r.Workload, r.SerialMS)
	fmt.Fprintf(&b, "%-7s %-9s %12s %9s %10s %8s\n", "workers", "steal", "elapsed(ms)", "speedup", "efficiency", "correct")
	dump := func(rs []ThreadResult) {
		for _, x := range rs {
			fmt.Fprintf(&b, "%-7d %-9s %12.2f %9.2fx %9.1f%% %8v\n",
				x.Workers, x.StealMode, x.ElapsedMS, x.Speedup, x.Efficiency*100, x.Correct)
		}
	}
	dump(r.HalfResults)
	dump(r.OneResults)
	return b.String()
}

// --- timing helpers: measure the serial reference separately so the
// correctness run and the timed run use identical code paths. ---

func timeGo(cfg GoConfig) time.Duration {
	start := time.Now()
	RunGoSerial(cfg)
	return time.Since(start)
}

func timeMerge(cfg MergeConfig) time.Duration {
	start := time.Now()
	RunMergeSerial(cfg)
	return time.Since(start)
}

func timeMatmul(cfg MatmulConfig) time.Duration {
	start := time.Now()
	RunMatmulSerial(cfg)
	return time.Since(start)
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }
