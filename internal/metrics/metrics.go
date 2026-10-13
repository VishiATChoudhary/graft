// Package metrics exposes process resource usage for tier /v1/info.
package metrics

import "syscall"

// CPUSeconds returns user+system CPU time consumed by this process.
func CPUSeconds() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return float64(ru.Utime.Sec+ru.Stime.Sec) + float64(ru.Utime.Usec+ru.Stime.Usec)/1e6
}

// MaxRSS returns peak resident memory in bytes.
func MaxRSS() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return maxRSSBytes(ru.Maxrss)
}
