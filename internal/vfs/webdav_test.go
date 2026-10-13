package vfs

import (
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/VishiATChoudhary/graft/internal/fetch"
	"github.com/VishiATChoudhary/graft/internal/store"
)

func TestWebDAV(t *testing.T) {
	s := store.NewMemLRU(1 << 30)
	root := buildTree(t, s, map[string]string{"a.txt": "a", "dir/b.txt": "b"})
	o, err := NewOverlay(New(fetch.StoreSource{Store: s}, root, time.Unix(1, 0), 1<<20), t.TempDir(), []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(WebDAVHandler(o))
	defer srv.Close()
	do := func(method, path, body string, hdr ...string) (int, string) {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := do("GET", "/dir/b.txt", ""); code != 200 || body != "b" {
		t.Fatalf("GET: %d %q", code, body)
	}
	if code, body := do("PROPFIND", "/dir/", "", "Depth", "1"); code != 207 || !strings.Contains(body, "b.txt") {
		t.Fatalf("PROPFIND: %d", code)
	}
	if code, _ := do("PUT", "/new.txt", "fresh"); code != 201 {
		t.Fatalf("PUT: %d", code)
	}
	if code, _ := do("PUT", "/a.txt", "changed"); code/100 != 2 {
		t.Fatalf("PUT existing: %d", code)
	}
	if code, _ := do("MKCOL", "/made/", ""); code != 201 {
		t.Fatalf("MKCOL: %d", code)
	}
	if code, _ := do("MOVE", "/dir/", "", "Destination", srv.URL+"/moved/"); code/100 != 2 {
		t.Fatalf("MOVE: %d", code)
	}
	ch, err := o.Changes()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range ch {
		st := "+"
		if c.Deleted {
			st = "-"
		}
		got = append(got, st+c.Path)
	}
	want := []string{"+a.txt", "-dir/b.txt", "+moved/b.txt", "+new.txt"}
	if !slices.Equal(got, want) {
		t.Fatalf("changes %v want %v", got, want)
	}
}
