// Package web serves the scheduler visualization: worker panels, queue
// lengths, a steal log, the task tree and speedup charts. It drives a real
// ws.Pool so every number on the page comes from live scheduling activity.
package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"sync"
	"time"

	"gosteal/internal/bench"
	"gosteal/internal/ws"
)

//go:embed static
var staticFS embed.FS

// Server owns the pool used by the demo workloads and serializes run
// requests so only one workload executes at a time.
type Server struct {
	mu      sync.Mutex
	pool    *ws.Pool
	workers int

	runMu       sync.Mutex // serializes workload execution
	infoMu      sync.RWMutex
	lastRun     *RunInfo
	speedupOnce sync.Mutex
	speedup     []bench.ThreadResult
}

// RunInfo summarizes the most recent demo execution.
type RunInfo struct {
	Workload   string           `json:"workload"`
	StartedAt  time.Time        `json:"startedAt"`
	ElapsedMS  float64          `json:"elapsedMs"`
	Workers    int              `json:"workers"`
	StealMode  string           `json:"stealMode"`
	Correct    bool             `json:"correct"`
	Error      string           `json:"error,omitempty"`
	TaskCount  int              `json:"taskCount"`
	StealCount int64            `json:"stealCount"`
	GoSummary  []bench.GoResult `json:"goSummary,omitempty"`
}

// NewServer builds a server with the given worker count.
func NewServer(workers int) *Server {
	if workers < 1 {
		workers = 8
	}
	s := &Server{workers: workers}
	s.pool = ws.NewPool(workers)
	return s
}

// Close shuts down the embedded pool.
func (s *Server) Close() { s.pool.Shutdown() }

// Handler returns the HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/state", s.handleState)
	mux.HandleFunc("/api/run", s.handleRun)
	mux.HandleFunc("/api/steal-mode", s.handleStealMode)
	mux.HandleFunc("/api/speedup", s.handleSpeedup)
	mux.HandleFunc("/api/info", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"workers": s.workers,
			"mode":    s.pool.StealMode().String(),
		})
	})

	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))
	return mux
}

// StateResponse is the full polling payload for the dashboard.
type StateResponse struct {
	Workers   []ws.WorkerStats `json:"workers"`
	Monitor   ws.Snapshot      `json:"monitor"`
	LastRun   *RunInfo         `json:"lastRun"`
	StealMode string           `json:"stealMode"`
	TimeShare []TimeShare      `json:"timeShare"`
	ServerMS  int64            `json:"serverMs"`
}

// TimeShare is one worker's busy/steal/idle breakdown as fractions.
type TimeShare struct {
	ID    int     `json:"id"`
	Busy  float64 `json:"busy"`
	Steal float64 `json:"steal"`
	Idle  float64 `json:"idle"`
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	stats := s.pool.Stats()
	shares := make([]TimeShare, len(stats))
	for i, st := range stats {
		total := st.BusyNanos + st.StealNanos + st.IdleNanos
		if total <= 0 {
			total = 1
		}
		shares[i] = TimeShare{
			ID:    st.ID,
			Busy:  float64(st.BusyNanos) / float64(total),
			Steal: float64(st.StealNanos) / float64(total),
			Idle:  float64(st.IdleNanos) / float64(total),
		}
	}
	s.infoMu.RLock()
	last := s.lastRun
	s.infoMu.RUnlock()
	writeJSON(w, http.StatusOK, StateResponse{
		Workers:   stats,
		Monitor:   s.pool.Monitor().Snapshot(),
		LastRun:   last,
		StealMode: s.pool.StealMode().String(),
		TimeShare: shares,
		ServerMS:  time.Now().UnixMilli(),
	})
}

type runRequest struct {
	Workload string `json:"workload"`
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	// Serialize runs and reset monitoring for a clean tree.
	go func() { s.runMu.Lock(); defer s.runMu.Unlock(); s.execute(req.Workload) }()
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "started"})
}

func (s *Server) execute(workload string) {
	s.pool.Monitor().Reset()
	info := &RunInfo{
		Workload:  workload,
		StartedAt: time.Now(),
		Workers:   s.workers,
		StealMode: s.pool.StealMode().String(),
	}
	defer func() {
		info.ElapsedMS = float64(time.Since(info.StartedAt).Microseconds()) / 1000.0
		snap := s.pool.Monitor().Snapshot()
		info.TaskCount = len(snap.Tasks)
		info.StealCount = snap.TotalSteals
		// runMu is already held by the caller; lastRun gets its own lock.
		s.infoMu.Lock()
		s.lastRun = info
		s.infoMu.Unlock()
	}()

	var err error
	switch workload {
	case "go":
		cfg := bench.DefaultGoConfig()
		cfg.Playouts = 12 // demo-sized
		serial := bench.RunGoSerial(cfg)
		got, e := bench.RunGoParallel(s.pool, cfg)
		err = e
		info.Correct = e == nil && goEqual(got, serial)
		info.GoSummary = summarizeGo(got)
	case "merge":
		cfg := bench.DefaultMergeConfig()
		serial := bench.RunMergeSerial(cfg)
		got, e := bench.RunMergeParallel(s.pool, cfg)
		err = e
		info.Correct = e == nil && intsEqual(got, serial)
	case "matmul":
		cfg := bench.DefaultMatmulConfig()
		serial := bench.RunMatmulSerial(cfg)
		got, e := bench.RunMatmulParallel(s.pool, cfg)
		err = e
		info.Correct = e == nil && intsEqual(got, serial)
	default:
		err = fmt.Errorf("unknown workload %q", workload)
	}
	if err != nil {
		info.Error = err.Error()
	}
}

type modeRequest struct {
	Mode string `json:"mode"`
}

func (s *Server) handleStealMode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req modeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	switch req.Mode {
	case "half":
		s.pool.SetStealMode(ws.StealHalf)
	case "one":
		s.pool.SetStealMode(ws.StealOne)
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "mode must be half|one"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"mode": s.pool.StealMode().String()})
}

// handleSpeedup runs a fresh 1..N sweep for the requested workload using
// short-lived pools (the demo pool stays untouched) and caches the result.
func (s *Server) handleSpeedup(w http.ResponseWriter, r *http.Request) {
	workload := r.URL.Query().Get("workload")
	if workload == "" {
		workload = "matmul"
	}
	s.speedupOnce.Lock()
	defer s.speedupOnce.Unlock()

	var results []bench.ThreadResult
	var serialMS float64
	switch workload {
	case "go":
		cfg := bench.DefaultGoConfig()
		cfg.Playouts = 6
		rep, err := bench.SweepGo(s.workers, cfg)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		results, serialMS = rep.HalfResults, rep.SerialMS
	case "merge":
		rep, err := bench.SweepMerge(s.workers, bench.DefaultMergeConfig())
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		results, serialMS = rep.HalfResults, rep.SerialMS
	default:
		rep, err := bench.SweepMatmul(s.workers, bench.DefaultMatmulConfig())
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		results, serialMS = rep.HalfResults, rep.SerialMS
	}
	s.speedup = results
	writeJSON(w, http.StatusOK, map[string]any{
		"serialMs": serialMS,
		"results":  results,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func intsEqual(a, b []int64) bool {
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

func goEqual(a, b []bench.GoResult) bool {
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

func summarizeGo(in []bench.GoResult) []bench.GoResult {
	if len(in) <= 12 {
		return in
	}
	return in[:12]
}
