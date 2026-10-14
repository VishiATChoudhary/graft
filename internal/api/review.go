package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/VishiATChoudhary/graft/internal/acl"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
	"github.com/VishiATChoudhary/graft/internal/review"
)

// Code review gates. Review rules in the policy require approvals before
// changes under a path can land, except classes of change the rule
// exempts: comment-only, whitespace-only or documentation changes are
// recognised semantically, so reviewers only see changes that can alter
// behaviour. Approvals are recorded per changeset (refs under reviews/),
// so pushing a new changeset dismisses earlier approvals, and authors
// cannot approve their own changes.

var errReviewRequired = errors.New("review required")

// RuleStatus reports one review rule's state for a change.
type RuleStatus struct {
	Path      string   `json:"path"`
	Required  int      `json:"required"`
	Approvals []string `json:"approvals"`
	Files     []string `json:"files"` // changed files needing this review
	Exempted  []string `json:"exempted,omitempty"`
	Satisfied bool     `json:"satisfied"`
}

// ReviewStatus is the result of evaluating review rules for a changeset.
type ReviewStatus struct {
	Changeset object.ID    `json:"changeset"`
	Base      object.ID    `json:"base"`
	Creator   string       `json:"creator,omitempty"`
	Rules     []RuleStatus `json:"rules"`
	Satisfied bool         `json:"satisfied"`
}

func approvalRef(cs object.ID, who string) string { return "reviews/" + cs.String() + "/" + who }

func (s *Server) approvals(cs object.ID) ([]string, error) {
	refs, err := s.Repo.Refs()
	if err != nil {
		return nil, err
	}
	prefix := "reviews/" + cs.String() + "/"
	var out []string
	for name := range refs {
		if who, ok := strings.CutPrefix(name, prefix); ok {
			out = append(out, who)
		}
	}
	sort.Strings(out)
	return out, nil
}

// reviewStatus evaluates review rules for landing head onto a ref whose
// tip is tip (Zero if none).
func (s *Server) reviewStatus(head, tip object.ID) (*ReviewStatus, error) {
	st := &ReviewStatus{Changeset: head, Satisfied: true}
	if s.ACL == nil || len(s.ACL.Reviews) == 0 {
		return st, nil
	}
	if v, err := s.Repo.DB.GetDerived("creator", head); err == nil {
		st.Creator = string(v)
	}
	if !tip.IsZero() {
		base, err := s.Repo.MergeBase(head, tip)
		if err == nil {
			st.Base = base
		}
	}
	var baseRoot object.ID
	var err error
	if !st.Base.IsZero() {
		if baseRoot, err = s.Repo.RootTree(st.Base); err != nil {
			return nil, err
		}
	}
	headRoot, err := s.Repo.RootTree(head)
	if err != nil {
		return nil, err
	}
	changes, err := s.Repo.DiffTrees(baseRoot, headRoot, nil)
	if err != nil {
		return nil, err
	}
	byRule := map[*acl.ReviewRule]*RuleStatus{}
	read := func(m object.Mode, id object.ID) []byte {
		if m == object.ModeNone {
			return nil
		}
		b, err := s.Repo.ReadBlob(id)
		if err != nil {
			return nil
		}
		return b
	}
	for _, c := range changes {
		rule := s.ACL.ReviewRuleFor(c.Path)
		if rule == nil || rule.Approvals <= 0 {
			continue
		}
		rs := byRule[rule]
		if rs == nil {
			rs = &RuleStatus{Path: rule.Path, Required: rule.Approvals}
			byRule[rule] = rs
		}
		class := review.Classify(c.Path, read(c.OldMode, c.OldBlob), read(c.NewMode, c.NewBlob))
		if review.Exempt(class, rule.Exempt) {
			rs.Exempted = append(rs.Exempted, c.Path+" ("+string(class)+")")
			continue
		}
		rs.Files = append(rs.Files, c.Path)
	}
	approvers, err := s.approvals(head)
	if err != nil {
		return nil, err
	}
	for rule, rs := range byRule {
		for _, who := range approvers {
			if who != st.Creator && s.ACL.CanApprove(s.ACL.PrincipalByName(who), rule) {
				rs.Approvals = append(rs.Approvals, who)
			}
		}
		rs.Satisfied = len(rs.Files) == 0 || len(rs.Approvals) >= rs.Required
		st.Satisfied = st.Satisfied && rs.Satisfied
		st.Rules = append(st.Rules, *rs)
	}
	sort.Slice(st.Rules, func(i, j int) bool { return st.Rules[i].Path < st.Rules[j].Path })
	return st, nil
}

// requireReviews fails unless head's review rules are satisfied for
// landing onto tip.
func (s *Server) requireReviews(head, tip object.ID) error {
	st, err := s.reviewStatus(head, tip)
	if err != nil {
		return err
	}
	if st.Satisfied {
		return nil
	}
	var missing []string
	for _, r := range st.Rules {
		if !r.Satisfied {
			missing = append(missing, fmt.Sprintf("%s needs %d approval(s) (has %d) for %s", r.Path, r.Required, len(r.Approvals), strings.Join(r.Files, ", ")))
		}
	}
	return fmt.Errorf("%w: %w: %s", errForbidden, errReviewRequired, strings.Join(missing, "; "))
}

// recordCreator remembers which principal created a changeset.
func (s *Server) recordCreator(ctx context.Context, id object.ID) {
	if s.ACL == nil {
		return
	}
	if p := s.principal(ctx); p.Name != "" {
		s.Repo.DB.PutDerived("creator", id, []byte(p.Name))
	}
}

func (s *Server) approve(ctx context.Context, rev string) (*ReviewStatus, error) {
	id, err := s.Repo.Resolve(rev)
	if err != nil {
		return nil, err
	}
	p := s.principal(ctx)
	if s.ACL != nil && p.Name == "" {
		return nil, fmt.Errorf("%w: sign in to approve", errForbidden)
	}
	who := p.Name
	if who == "" {
		who = "anonymous"
	}
	if v, err := s.Repo.DB.GetDerived("creator", id); err == nil && string(v) == who {
		return nil, fmt.Errorf("%w: you cannot approve your own change", errForbidden)
	}
	if err := s.Repo.UpdateRef(approvalRef(id, who), object.Zero, id); err != nil && !errors.Is(err, repo.ErrStaleRef) {
		return nil, err
	}
	go s.processQueue() // a queued change request may now be landable
	tip, _ := s.Repo.Ref("main")
	return s.reviewStatus(id, tip)
}
