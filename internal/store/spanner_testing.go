package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"cloud.google.com/go/spanner/spannertest"
	"cloud.google.com/go/spanner/spansql"
)

var fakeSpannerN atomic.Int64

// OpenFakeSpanner starts an in-memory Spanner (cloud.google.com/go/spanner/spannertest)
// with graft's schema and returns a backend on it, for tests.
func OpenFakeSpanner(blobs string) (*Spanner, func(), error) {
	srv, err := spannertest.NewServer("localhost:0")
	if err != nil {
		return nil, nil, err
	}
	ddl, err := spansql.ParseDDL("schema", strings.Join(SpannerSchema, ";\n"))
	if err != nil {
		srv.Close()
		return nil, nil, err
	}
	if err := srv.UpdateDDL(ddl); err != nil {
		srv.Close()
		return nil, nil, err
	}
	os.Setenv("SPANNER_EMULATOR_HOST", srv.Addr)
	db := fmt.Sprintf("projects/p/instances/i/databases/d%d", fakeSpannerN.Add(1))
	s, err := OpenSpanner(context.Background(), db, 64<<20, blobs)
	if err != nil {
		srv.Close()
		return nil, nil, err
	}
	return s, func() { s.Close(); srv.Close() }, nil
}
