package api

import (
	"bytes"
	"encoding/json"
	"hash/fnv"
	"io"
	"net/http"
	"time"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
)

// Ref affinity. Lands on one ref are coalesced inside a server (one CAS
// publishes a batch), but separate API servers would still race each
// other's batches on the same CAS. With Peers set, a land is forwarded to
// the peer that owns the ref (by hash of its name), so a ref's lands meet
// in one place. Ownership is only a routing hint: correctness still rests
// on the CAS, and if the owner is unreachable the land runs locally.
// The owner re-authenticates the forwarded credentials and applies its own
// review and permission checks.

const forwardedHeader = "X-Graft-Forwarded"

// Many lands are forwarded concurrently: keep enough idle connections that
// they are reused rather than churned through TIME_WAIT.
var peerClient = &http.Client{Timeout: 3 * time.Minute, Transport: &http.Transport{
	MaxIdleConns: 1024, MaxIdleConnsPerHost: 256, IdleConnTimeout: 90 * time.Second}}

// forwardedError carries a peer's error response through unchanged.
type forwardedError struct {
	code int
	body []byte
}

func (e *forwardedError) Error() string { return string(e.body) }

func (s *Server) owner(ref string) int {
	h := fnv.New32a()
	h.Write([]byte(ref))
	return int(h.Sum32() % uint32(len(s.Peers)))
}

// landRouted lands id on ref here or at the ref's owner.
func (s *Server) landRouted(r *http.Request, ref string, id object.ID, author string, when int64) (*repo.LandResult, error) {
	if len(s.Peers) > 1 && r.Header.Get(forwardedHeader) == "" {
		if o := s.owner(ref); o != s.Self {
			res, err := s.forwardLand(r, s.Peers[o], ref, id, author)
			if _, ok := err.(*forwardedError); ok || err == nil {
				return res, err
			}
			// Owner unreachable: land here; the CAS keeps it correct.
		}
	}
	if s.ACL != nil && len(s.ACL.Reviews) > 0 {
		tip, _ := s.Repo.Ref(ref)
		if err := s.requireReviews(id, tip); err != nil {
			return nil, err
		}
	}
	return s.Repo.Land(ref, id, author, when, 0, s.checkRefMove(r.Context()))
}

func (s *Server) forwardLand(r *http.Request, peer, ref string, id object.ID, author string) (*repo.LandResult, error) {
	body, _ := json.Marshal(map[string]string{"Ref": ref, "ID": id.String(), "Author": author})
	req, err := http.NewRequestWithContext(r.Context(), "POST", peer+"/v1/land", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(forwardedHeader, "1")
	if a := r.Header.Get("Authorization"); a != "" {
		req.Header.Set("Authorization", a)
	}
	resp, err := peerClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &forwardedError{resp.StatusCode, b}
	}
	var res repo.LandResult
	if err := json.Unmarshal(b, &res); err != nil {
		return nil, err
	}
	return &res, nil
}
