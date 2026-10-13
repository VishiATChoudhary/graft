//go:build !darwin && !linux && !windows

package watch

import (
	"context"
	"errors"
)

func start(context.Context, string, *Journal) (func(), error) {
	return nil, errors.New("filesystem watching is supported on macOS and Linux")
}
