// Command dashboard serves the work-stealing scheduler visualization.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"gosteal/internal/web"
)

func main() {
	workers := flag.Int("workers", 8, "number of worker threads in the demo pool")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	server := web.NewServer(*workers)
	defer server.Close()

	log.Printf("scheduler dashboard listening on http://localhost%s (workers=%d)", *addr, *workers)
	if err := http.ListenAndServe(*addr, server.Handler()); err != nil {
		log.Fatal(fmt.Errorf("server: %w", err))
	}
}
