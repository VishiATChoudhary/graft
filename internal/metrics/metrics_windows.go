package metrics

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

// CPUSeconds returns user+kernel CPU time consumed by this process.
func CPUSeconds() float64 {
	var c, e, k, u windows.Filetime
	if err := windows.GetProcessTimes(windows.CurrentProcess(), &c, &e, &k, &u); err != nil {
		return 0
	}
	ticks := func(f windows.Filetime) float64 {
		return float64(uint64(f.HighDateTime)<<32|uint64(f.LowDateTime)) / 1e7
	}
	return ticks(k) + ticks(u)
}

type processMemoryCounters struct {
	cb                         uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uintptr
	WorkingSetSize             uintptr
	QuotaPeakPagedPoolUsage    uintptr
	QuotaPagedPoolUsage        uintptr
	QuotaPeakNonPagedPoolUsage uintptr
	QuotaNonPagedPoolUsage     uintptr
	PagefileUsage              uintptr
	PeakPagefileUsage          uintptr
}

var getProcessMemoryInfo = windows.NewLazySystemDLL("psapi.dll").NewProc("GetProcessMemoryInfo")

// MaxRSS returns the peak working set in bytes.
func MaxRSS() int64 {
	var m processMemoryCounters
	m.cb = uint32(unsafe.Sizeof(m))
	r, _, _ := getProcessMemoryInfo.Call(uintptr(windows.CurrentProcess()), uintptr(unsafe.Pointer(&m)), uintptr(m.cb))
	if r == 0 {
		return 0
	}
	return int64(m.PeakWorkingSetSize)
}
