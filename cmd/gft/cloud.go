//go:build cloud

package main

import (
	"context"

	"github.com/VishiATChoudhary/graft/internal/store"
)

func init() {
	openSpanner = func(ctx context.Context, db string, mem int64, blobs string, init bool) (store.Backend, error) {
		if init {
			if err := store.CreateSpannerSchema(ctx, db); err != nil {
				return nil, err
			}
		}
		return store.OpenSpanner(ctx, db, mem, blobs)
	}
}
