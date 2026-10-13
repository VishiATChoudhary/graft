//go:build !darwin

package metrics

func maxRSSBytes(v int64) int64 { return v * 1024 } // KiB on linux
