package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/VishiATChoudhary/graft/internal/fetch"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// Client talks to a graft server's API.
type Client struct {
	*fetch.Remote
}

func NewClient(endpoint string) *Client { return &Client{fetch.NewRemote(endpoint)} }

// APIError carries the server's status and message.
type APIError struct {
	Status    int
	Message   string
	Conflicts []string
}

func (e *APIError) Error() string { return e.Message }

func (e *APIError) Unwrap() error {
	switch e.Status {
	case http.StatusConflict:
		return repo.ErrConflict
	case http.StatusPreconditionFailed:
		return repo.ErrStaleRef
	case http.StatusNotFound:
		return object.ErrNotFound
	}
	return nil
}

func (c *Client) do(ctx context.Context, method, path string, q url.Values, body any, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	u := c.Base() + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTPClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var cb ConflictBody
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if json.Unmarshal(raw, &cb) != nil || cb.Error == "" {
			cb.Error = fmt.Sprintf("%s: %s", resp.Status, bytes.TrimSpace(raw))
		}
		return &APIError{Status: resp.StatusCode, Message: cb.Error, Conflicts: cb.Conflicts}
	}
	if w, ok := out.(io.Writer); ok {
		_, err = io.Copy(w, resp.Body)
		return err
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) Resolve(ctx context.Context, rev string) (*ChangesetInfo, error) {
	var ci ChangesetInfo
	return &ci, c.do(ctx, "GET", "/v1/resolve", url.Values{"rev": {rev}}, nil, &ci)
}

func (c *Client) Changeset(ctx context.Context, rev string) (*ChangesetInfo, error) {
	var ci ChangesetInfo
	return &ci, c.do(ctx, "GET", "/v1/changeset", url.Values{"id": {rev}}, nil, &ci)
}

func (c *Client) Refs(ctx context.Context) (map[string]object.ID, error) {
	var m map[string]object.ID
	return m, c.do(ctx, "GET", "/v1/refs", nil, nil, &m)
}

func (c *Client) UpdateRef(ctx context.Context, name string, old, new object.ID) error {
	return c.do(ctx, "POST", "/v1/refs/update", nil, RefUpdate{name, old, new}, nil)
}

func (c *Client) Create(ctx context.Context, req *CreateRequest) (*CreateResponse, error) {
	var out CreateResponse
	return &out, c.do(ctx, "POST", "/v1/changesets", nil, req, &out)
}

func (c *Client) Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	var out WriteResponse
	return &out, c.do(ctx, "POST", "/v1/write", nil, req, &out)
}

func (c *Client) Log(ctx context.Context, rev, path string, limit int, firstParent, follow bool) ([]*ChangesetInfo, error) {
	q := url.Values{"rev": {rev}, "limit": {fmt.Sprint(limit)}}
	if follow {
		q.Set("follow", "1")
	}
	if path != "" {
		q.Set("path", path)
	}
	if firstParent {
		q.Set("first_parent", "1")
	}
	var out []*ChangesetInfo
	return out, c.do(ctx, "GET", "/v1/log", q, nil, &out)
}

func (c *Client) Blame(ctx context.Context, rev, path string) ([]BlameEntry, error) {
	var out []BlameEntry
	return out, c.do(ctx, "GET", "/v1/blame", url.Values{"rev": {rev}, "path": {path}}, nil, &out)
}

type DiffResult struct {
	From    object.ID         `json:"from"`
	To      object.ID         `json:"to"`
	Changes []repo.FileChange `json:"changes"`
}

func diffQuery(from, to string, paths []string) url.Values {
	q := url.Values{"to": {to}}
	if from != "" {
		q.Set("from", from)
	}
	for _, p := range paths {
		q.Add("path", p)
	}
	return q
}

func (c *Client) Diff(ctx context.Context, from, to string, paths []string) (*DiffResult, error) {
	var out DiffResult
	return &out, c.do(ctx, "GET", "/v1/diff", diffQuery(from, to, paths), nil, &out)
}

func (c *Client) Patch(ctx context.Context, w io.Writer, from, to string, paths []string) error {
	q := diffQuery(from, to, paths)
	q.Set("patch", "1")
	return c.do(ctx, "GET", "/v1/diff", q, nil, w)
}

func (c *Client) MergeBases(ctx context.Context, a, b string) ([]object.ID, error) {
	var out struct{ Bases []object.ID }
	return out.Bases, c.do(ctx, "GET", "/v1/mergebase", url.Values{"a": {a}, "b": {b}}, nil, &out)
}

func (c *Client) IsAncestor(ctx context.Context, a, b string) (bool, error) {
	var out struct{ Ancestor bool }
	return out.Ancestor, c.do(ctx, "GET", "/v1/isancestor", url.Values{"a": {a}, "b": {b}}, nil, &out)
}

func (c *Client) Rebase(ctx context.Context, id, onto string) (object.ID, error) {
	var out struct{ ID object.ID }
	return out.ID, c.do(ctx, "POST", "/v1/rebase", nil, map[string]string{"id": id, "onto": onto}, &out)
}

func (c *Client) Merge(ctx context.Context, a, b, author, msg string) (object.ID, error) {
	var out struct{ ID object.ID }
	return out.ID, c.do(ctx, "POST", "/v1/merge", nil, map[string]string{"a": a, "b": b, "author": author, "message": msg}, &out)
}

func (c *Client) Land(ctx context.Context, ref, id, author string) (*repo.LandResult, error) {
	var out repo.LandResult
	return &out, c.do(ctx, "POST", "/v1/land", nil, map[string]string{"ref": ref, "id": id, "author": author}, &out)
}

func (c *Client) Ls(ctx context.Context, rev, path string) ([]object.TreeEntry, error) {
	var out []object.TreeEntry
	return out, c.do(ctx, "GET", "/v1/ls", url.Values{"rev": {rev}, "path": {path}}, nil, &out)
}

func (c *Client) Cat(ctx context.Context, w io.Writer, rev, path string) error {
	return c.do(ctx, "GET", "/v1/cat", url.Values{"rev": {rev}, "path": {path}}, nil, w)
}

func (c *Client) Search(ctx context.Context, rev, dir, q string, limit int) ([]string, error) {
	var out []string
	return out, c.do(ctx, "GET", "/v1/search", url.Values{"rev": {rev}, "path": {dir}, "q": {q}, "limit": {fmt.Sprint(limit)}}, nil, &out)
}

func (c *Client) Info(ctx context.Context) (map[string]any, error) {
	var out map[string]any
	return out, c.do(ctx, "GET", "/v1/info", nil, nil, &out)
}

// Missing returns which of ids the server lacks.
func (c *Client) Missing(ctx context.Context, ids []object.ID) ([]object.ID, error) {
	var out []object.ID
	for start := 0; start < len(ids); start += fetch.MaxBatch {
		end := min(start+fetch.MaxBatch, len(ids))
		body := make([]byte, 0, (end-start)*32)
		for _, id := range ids[start:end] {
			body = append(body, id[:]...)
		}
		req, err := http.NewRequestWithContext(ctx, "POST", c.Base()+"/v1/objects/missing", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		resp, err := c.HTTPClient().Do(req)
		if err != nil {
			return nil, err
		}
		raw, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("missing: %s", resp.Status)
		}
		for i := 0; i+32 <= len(raw); i += 32 {
			var id object.ID
			copy(id[:], raw[i:])
			out = append(out, id)
		}
	}
	return out, nil
}

// PutObjects uploads stored objects (blobs) read from src.
func (c *Client) PutObjects(ctx context.Context, src store.Objects, ids []object.ID) error {
	const chunk = 32 << 20
	for len(ids) > 0 {
		pr, pw := io.Pipe()
		done := make(chan int, 1)
		go func() {
			sent := 0
			defer func() { done <- sent }()
			bw := bufio.NewWriterSize(pw, 256<<10)
			var hdr [36]byte
			total := 0
			for sent < len(ids) && total < chunk {
				id := ids[sent]
				b, err := src.Get(id)
				if err != nil {
					pw.CloseWithError(err)
					return
				}
				copy(hdr[:32], id[:])
				binary.BigEndian.PutUint32(hdr[32:], uint32(len(b)))
				bw.Write(hdr[:])
				bw.Write(b)
				total += len(b) + 36
				sent++
			}
			pw.CloseWithError(bw.Flush())
		}()
		req, err := http.NewRequestWithContext(ctx, "POST", c.Base()+"/v1/objects/put", pr)
		if err != nil {
			return err
		}
		resp, err := c.HTTPClient().Do(req)
		if err != nil {
			pr.CloseWithError(err)
			<-done
			return err
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		pr.Close()
		sent := <-done
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("put objects: %s: %s", resp.Status, bytes.TrimSpace(raw))
		}
		ids = ids[sent:]
	}
	return nil
}

// IsConflict extracts conflict paths from an API error.
func IsConflict(err error) ([]string, bool) {
	var ae *APIError
	if errors.As(err, &ae) && ae.Status == http.StatusConflict {
		return ae.Conflicts, true
	}
	return nil, false
}
