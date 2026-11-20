//go:build !arm64 && !amd64

package ws

import "runtime"

// Fallback for other architectures: bounded spinning via Gosched.
func procyield(n int) {
	for i := 0; i < n; i++ {
		runtime.Gosched()
	}
}
