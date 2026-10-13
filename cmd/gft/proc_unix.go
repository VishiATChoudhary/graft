//go:build unix

package main

import (
	"os"
	"os/exec"
	"syscall"
)

var stopSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}

func detach(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} }

func terminate(pid int) error { return syscall.Kill(pid, syscall.SIGTERM) }
