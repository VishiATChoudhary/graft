// Package acl implements repository and directory-level access control.
//
// A policy maps bearer tokens to principals, principals to groups, and
// grants roles at the repository level, with path rules that restrict who
// may read or write individual directories. The most specific matching
// rule wins for each permission; a rule that does not mention a permission
// inherits it from the enclosing rule (or the repository default).
//
// Example:
//
//	{
//	  "tokens":    {"sha256:<hex of token>": "alice", "t0k3n": "ci-bot"},
//	  "groups":    {"payments": ["alice"], "everyone": ["*"]},
//	  "anonymous": "none",
//	  "default":   "write",
//	  "admins":    ["alice"],
//	  "rules": [
//	    {"path": "secrets",           "read":  ["group:security"], "write": ["group:security"]},
//	    {"path": "services/payments", "write": ["group:payments"]}
//	  ]
//	}
package acl

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Role is a repository-level grant.
type Role string

const (
	RoleNone  Role = "none"
	RoleRead  Role = "read"
	RoleWrite Role = "write"
	RoleAdmin Role = "admin"
)

func (r Role) rank() int {
	switch r {
	case RoleRead:
		return 1
	case RoleWrite:
		return 2
	case RoleAdmin:
		return 3
	}
	return 0
}

// Rule restricts access below a path. Nil lists inherit.
type Rule struct {
	Path  string   `json:"path"`
	Read  []string `json:"read,omitempty"`
	Write []string `json:"write,omitempty"`
}

// Policy is a repository's access control configuration.
type Policy struct {
	Tokens    map[string]string   `json:"tokens"`
	Groups    map[string][]string `json:"groups"`
	Anonymous Role                `json:"anonymous"`
	Default   Role                `json:"default"` // for authenticated principals
	Roles     map[string]Role     `json:"roles"`   // per-principal or "group:x" overrides
	Admins    []string            `json:"admins"`
	Rules     []Rule              `json:"rules"`
}

// Load reads a policy file.
func Load(path string) (*Policy, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p Policy
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("acl %s: %w", path, err)
	}
	return p.normalize(), nil
}

func (p *Policy) normalize() *Policy {
	if p.Anonymous == "" {
		p.Anonymous = RoleNone
	}
	if p.Default == "" {
		p.Default = RoleWrite
	}
	for i := range p.Rules {
		p.Rules[i].Path = strings.Trim(p.Rules[i].Path, "/")
	}
	// Longest paths first, so the first match is the most specific.
	sort.SliceStable(p.Rules, func(i, j int) bool { return len(p.Rules[i].Path) > len(p.Rules[j].Path) })
	return p
}

// Principal is an authenticated caller ("" for anonymous).
type Principal struct {
	Name   string
	groups map[string]bool
}

// Anonymous is the unauthenticated principal.
var Anonymous = &Principal{}

// Authenticate maps a bearer token to a principal. Tokens may be stored in
// the policy in plain text or as "sha256:<hex>".
func (p *Policy) Authenticate(token string) *Principal {
	if token == "" {
		return Anonymous
	}
	sum := sha256.Sum256([]byte(token))
	hashed := "sha256:" + hex.EncodeToString(sum[:])
	var name string
	for k, v := range p.Tokens {
		if subtle.ConstantTimeCompare([]byte(k), []byte(hashed)) == 1 || subtle.ConstantTimeCompare([]byte(k), []byte(token)) == 1 {
			name = v
		}
	}
	if name == "" {
		return Anonymous
	}
	pr := &Principal{Name: name, groups: map[string]bool{}}
	for g, members := range p.Groups {
		for _, m := range members {
			if m == name || m == "*" {
				pr.groups[g] = true
			}
		}
	}
	return pr
}

func (pr *Principal) matches(entries []string) bool {
	for _, e := range entries {
		switch {
		case e == "*":
			return pr.Name != ""
		case strings.HasPrefix(e, "group:"):
			if pr.groups[strings.TrimPrefix(e, "group:")] {
				return true
			}
		case e == pr.Name && pr.Name != "":
			return true
		}
	}
	return false
}

// RepoRole returns the principal's repository-level role.
func (p *Policy) RepoRole(pr *Principal) Role {
	if pr.Name == "" {
		return p.Anonymous
	}
	for _, a := range p.Admins {
		if pr.matches([]string{a}) {
			return RoleAdmin
		}
	}
	role := p.Default
	for who, r := range p.Roles {
		if pr.matches([]string{who}) && r.rank() > role.rank() {
			role = r
		}
	}
	return role
}

func under(path, prefix string) bool {
	return prefix == "" || path == prefix || strings.HasPrefix(path, prefix+"/")
}

// CanRead reports whether pr may read path ("" is the repository root).
func (p *Policy) CanRead(pr *Principal, path string) bool {
	if p.RepoRole(pr).rank() < RoleRead.rank() {
		return false
	}
	if p.RepoRole(pr) == RoleAdmin {
		return true
	}
	path = strings.Trim(path, "/")
	for _, r := range p.Rules {
		if r.Read != nil && under(path, r.Path) {
			return pr.matches(r.Read)
		}
	}
	return true
}

// CanWrite reports whether pr may change path.
func (p *Policy) CanWrite(pr *Principal, path string) bool {
	if p.RepoRole(pr).rank() < RoleWrite.rank() || !p.CanRead(pr, path) {
		return false
	}
	if p.RepoRole(pr) == RoleAdmin {
		return true
	}
	path = strings.Trim(path, "/")
	for _, r := range p.Rules {
		if r.Write != nil && under(path, r.Path) {
			return pr.matches(r.Write)
		}
	}
	return true
}

// ReadRestricted returns the rule paths whose contents are not readable by
// everyone with repository read access. Their subtrees are indexed so that
// object fetches can be checked.
func (p *Policy) ReadRestricted() []string {
	var out []string
	for _, r := range p.Rules {
		if r.Read != nil {
			out = append(out, r.Path)
		}
	}
	return out
}

// CanReadSubtree reports whether pr may read everything under prefix, as
// required to fetch the tree object of that directory.
func (p *Policy) CanReadSubtree(pr *Principal, prefix string) bool {
	if !p.CanRead(pr, prefix) {
		return false
	}
	for _, r := range p.Rules {
		if r.Read != nil && under(r.Path, prefix) && !pr.matches(r.Read) && p.RepoRole(pr) != RoleAdmin {
			return false
		}
	}
	return true
}
