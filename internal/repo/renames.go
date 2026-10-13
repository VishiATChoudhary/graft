package repo

import (
	"sort"

	"github.com/VishiATChoudhary/graft/internal/diff"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// Rename detection mirrors git's default (-M50%): a path added in a
// changeset is a rename of a path deleted in the same changeset if the
// contents are identical, or at least 50% similar by content. Detection runs
// once, when unodes are derived, and is recorded as Unode.CopyFrom, so
// queries never pay for it again.
const (
	renameThreshold = 0.5
	maxRenameInexact = 200     // per side; beyond this only exact renames are found
	maxRenameSize    = 1 << 20 // files larger than this only match exactly
	maxDeletedExpand = 10000   // files expanded from a deleted directory
)

type renameCand struct {
	path string
	blob object.ID
}

// detectRenames returns new path -> source file unode (in parent's history).
func (r *Repo) detectRenames(parent, parentUnode object.ID, changes []object.Change) (map[string]object.ID, error) {
	root, err := r.RootTree(parent)
	if err != nil {
		return nil, err
	}
	var adds, dels []renameCand
	for _, ch := range changes {
		e, lerr := r.Lookup(root, ch.Path)
		if ch.Mode == object.ModeNone {
			if lerr != nil {
				continue
			}
			if e.Mode == object.ModeDir {
				if err := r.expandFiles(ch.Path, e.ID, &dels); err != nil {
					return nil, err
				}
			} else if e.Mode != object.ModeGitlink {
				dels = append(dels, renameCand{ch.Path, e.ID})
			}
			continue
		}
		if ch.Mode == object.ModeGitlink || ch.Mode == object.ModeSymlink {
			continue
		}
		if lerr != nil || e.Mode == object.ModeDir {
			adds = append(adds, renameCand{ch.Path, ch.Blob})
		}
	}
	if len(adds) == 0 || len(dels) == 0 {
		return nil, nil
	}
	// Changes listing both "D dir" and "M dir/x" (e.g. deleteall) are not
	// renames of a path onto itself.
	added := map[string]bool{}
	for _, a := range adds {
		added[a.path] = true
	}
	src := map[string]string{} // new -> old path
	usedDel := map[string]bool{}
	byBlob := map[object.ID][]string{}
	for _, d := range dels {
		if !added[d.path] {
			byBlob[d.blob] = append(byBlob[d.blob], d.path)
		}
	}
	var restAdds []renameCand
	for _, a := range adds {
		if ps := byBlob[a.blob]; len(ps) > 0 {
			src[a.path] = ps[0]
			usedDel[ps[0]] = true
			byBlob[a.blob] = ps[1:]
			continue
		}
		restAdds = append(restAdds, a)
	}
	var restDels []renameCand
	for _, d := range dels {
		if !usedDel[d.path] && !added[d.path] {
			restDels = append(restDels, d)
		}
	}
	if len(restAdds) > 0 && len(restDels) > 0 && len(restAdds) <= maxRenameInexact && len(restDels) <= maxRenameInexact {
		if err := r.inexactRenames(restAdds, restDels, src); err != nil {
			return nil, err
		}
	}
	out := make(map[string]object.ID, len(src))
	for np, op := range src {
		id, _, err := r.lookupUnodeIn(parentUnode, op, true)
		if err != nil {
			continue
		}
		out[np] = id
	}
	return out, nil
}

func (r *Repo) expandFiles(dir string, tree object.ID, out *[]renameCand) error {
	t, err := store.ReadTree(r.DB, tree)
	if err != nil {
		return err
	}
	for _, e := range t.Entries {
		if len(*out) >= maxDeletedExpand {
			return nil
		}
		p := join(dir, e.Name)
		switch e.Mode {
		case object.ModeDir:
			if err := r.expandFiles(p, e.ID, out); err != nil {
				return err
			}
		case object.ModeFile, object.ModeExec:
			*out = append(*out, renameCand{p, e.ID})
		}
	}
	return nil
}

type lineProfile struct {
	counts map[string]int
	size   int
}

func profile(b []byte) lineProfile {
	p := lineProfile{counts: map[string]int{}, size: len(b)}
	for _, l := range diff.SplitLines(b) {
		p.counts[string(l)]++
	}
	return p
}

// similarity estimates the fraction of content shared by two files, like
// git's rename score: copied bytes / max(size).
func similarity(a lineProfile, b []byte) float64 {
	mx := max(a.size, len(b))
	if mx == 0 {
		return 1
	}
	left := make(map[string]int, len(a.counts))
	for k, v := range a.counts {
		left[k] = v
	}
	copied := 0
	for _, l := range diff.SplitLines(b) {
		if left[string(l)] > 0 {
			left[string(l)]--
			copied += len(l)
		}
	}
	return float64(copied) / float64(mx)
}

func (r *Repo) inexactRenames(adds, dels []renameCand, src map[string]string) error {
	type pair struct {
		a, d  int
		score float64
	}
	profiles := make([]*lineProfile, len(dels))
	sizes := make([]int, len(dels))
	for i, d := range dels {
		b, err := r.ReadBlob(d.blob)
		if err != nil {
			return err
		}
		sizes[i] = len(b)
		if len(b) <= maxRenameSize && !diff.IsBinary(b) {
			p := profile(b)
			profiles[i] = &p
		}
	}
	var pairs []pair
	for ai, a := range adds {
		b, err := r.ReadBlob(a.blob)
		if err != nil {
			return err
		}
		if len(b) > maxRenameSize || diff.IsBinary(b) {
			continue
		}
		for di := range dels {
			p := profiles[di]
			if p == nil {
				continue
			}
			// Cheap bound: sizes too different can't reach the threshold.
			if float64(min(sizes[di], len(b))) < renameThreshold*float64(max(sizes[di], len(b))) {
				continue
			}
			if s := similarity(*p, b); s >= renameThreshold {
				pairs = append(pairs, pair{ai, di, s})
			}
		}
	}
	sort.SliceStable(pairs, func(i, j int) bool { return pairs[i].score > pairs[j].score })
	usedA, usedD := map[int]bool{}, map[int]bool{}
	for _, p := range pairs {
		if usedA[p.a] || usedD[p.d] {
			continue
		}
		usedA[p.a], usedD[p.d] = true, true
		src[adds[p.a].path] = dels[p.d].path
	}
	return nil
}
