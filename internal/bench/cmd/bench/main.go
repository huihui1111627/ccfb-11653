// Command bench runs the work-stealing runtime workloads, verifies parallel
// results against serial references and reports speedups across worker
// counts and steal policies.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"gosteal/internal/bench"
)

func main() {
	var (
		workers  = flag.Int("workers", 8, "maximum worker threads (sweeps 1..N)")
		workload = flag.String("workload", "all", "go | merge | matmul | all")
		asJSON   = flag.Bool("json", false, "emit machine-readable JSON")
		outPath  = flag.String("out", "", "optional file to also write JSON results into")
	)
	flag.Parse()

	reports := runSweeps(*workers, *workload)
	if len(reports) == 0 {
		fmt.Fprintf(os.Stderr, "unknown workload %q\n", *workload)
		os.Exit(2)
	}

	allOK := true
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(reports); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	} else {
		for _, r := range reports {
			fmt.Print(r.String())
			fmt.Println()
		}
	}
	for _, r := range reports {
		if !r.AllCorrect() {
			allOK = false
		}
	}
	if !allOK {
		fmt.Fprintln(os.Stderr, "CORRECTNESS FAILURE: a parallel run did not match the serial result")
		os.Exit(1)
	}
	if *outPath != "" {
		data, err := json.MarshalIndent(reports, "", "  ")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := os.WriteFile(*outPath, data, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	fmt.Println("all parallel results match the serial reference exactly")
}

func runSweeps(max int, which string) []*bench.Report {
	var out []*bench.Report
	run := func(name string, fn func() (*bench.Report, error)) {
		if which != "all" && which != name {
			return
		}
		rep, err := fn()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
			os.Exit(1)
		}
		out = append(out, rep)
	}
	run("go", func() (*bench.Report, error) {
		return bench.SweepGo(max, bench.DefaultGoConfig())
	})
	run("merge", func() (*bench.Report, error) {
		return bench.SweepMerge(max, bench.DefaultMergeConfig())
	})
	run("matmul", func() (*bench.Report, error) {
		return bench.SweepMatmul(max, bench.DefaultMatmulConfig())
	})
	return out
}
