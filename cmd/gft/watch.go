package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"time"

	"github.com/VishiATChoudhary/graft/internal/client"
	"github.com/VishiATChoudhary/graft/internal/watch"
)

func cmdWatch(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	bg := fs.Bool("background", false, "run detached")
	stop := fs.Bool("stop", false, "stop a background watcher")
	parse(fs, args)
	root, err := client.Find(".")
	if err != nil {
		return err
	}
	pidFile := filepath.Join(root, ".gft", "watch.pid")
	if *stop {
		b, err := os.ReadFile(pidFile)
		if err != nil {
			return fmt.Errorf("no background watcher")
		}
		pid, _ := strconv.Atoi(string(b))
		os.Remove(pidFile)
		return terminate(pid)
	}
	if *bg {
		self, err := os.Executable()
		if err != nil {
			return err
		}
		logf, err := os.Create(filepath.Join(root, ".gft", "watch.log"))
		if err != nil {
			return err
		}
		cmd := exec.Command(self, "watch")
		cmd.Dir = root
		cmd.Stdout, cmd.Stderr = logf, logf
		detach(cmd)
		if err := cmd.Start(); err != nil {
			return err
		}
		os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644)
		for i := 0; i < 100; i++ {
			if watch.Dial(root) != nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		fmt.Printf("watching %s (pid %d)\n", root, cmd.Process.Pid)
		return nil
	}
	ctx, cancel := signal.NotifyContext(context.Background(), stopSignals...)
	defer cancel()
	fmt.Printf("watching %s\n", root)
	return watch.Run(ctx, root)
}
