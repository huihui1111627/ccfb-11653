package ws

// procyield spins for n pause/yield iterations without yielding the OS
// thread or entering the Go scheduler.
func procyield(n int)
