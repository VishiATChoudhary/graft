package repo

import (
	"fmt"

	"github.com/VishiATChoudhary/graft/internal/diff"
	"github.com/VishiATChoudhary/graft/internal/object"
)

// BlameLine attributes one line of a file.
type BlameLine struct {
	Changeset object.ID `json:"changeset"`
	Line      int       `json:"line"` // 1-based line number in the attributed changeset's version
}

// Blame is derived per file version (path, link seq) and stored as runs of
// consecutive lines sharing a changeset, with their original line numbers.
// A version's blame is computed from its previous versions' blame plus a
// line diff, so each version is diffed once, ever.
type blameRun struct {
	n    int
	seq  uint64
	orig int // 0-based line number in seq's version of the file
}

func encodeBlame(runs []blameRun) []byte {
	var e object.Encoder
	e.Uvarint(uint64(len(runs)))
	for _, r := range runs {
		e.Uvarint(uint64(r.n))
		e.Uvarint(r.seq)
		e.Uvarint(uint64(r.orig))
	}
	return e.B
}

func decodeBlame(b []byte) ([]blameRun, error) {
	d := object.Decoder{B: b}
	n := d.Uvarint()
	if n > uint64(len(b)) {
		return nil, object.ErrCorrupt
	}
	runs := make([]blameRun, 0, n)
	for i := uint64(0); i < n && d.Err == nil; i++ {
		runs = append(runs, blameRun{int(d.Uvarint()), d.Uvarint(), int(d.Uvarint())})
	}
	return runs, d.Err
}

func expandBlame(runs []blameRun) []blameRun {
	var out []blameRun
	for _, r := range runs {
		for k := 0; k < r.n; k++ {
			out = append(out, blameRun{1, r.seq, r.orig + k})
		}
	}
	return out
}

func compressBlame(lines []blameRun) []blameRun {
	var out []blameRun
	for _, l := range lines {
		if n := len(out); n > 0 && out[n-1].seq == l.seq && out[n-1].orig+out[n-1].n == l.orig {
			out[n-1].n++
			continue
		}
		out = append(out, l)
	}
	return out
}

// registerVersion records how to decode a version key, so the derived-data
// engine (which works on IDs) can find a version's parents.
func (r *Repo) registerVersion(path string, link uint64) (object.ID, error) {
	k := versionKey(path, link)
	if _, err := r.DB.GetDerived("version", k); err == nil {
		return k, nil
	}
	return k, r.DB.PutDerived("version", k, encodeVersion(path, link))
}

func (r *Repo) versionOf(k object.ID) (string, uint64, error) {
	v, err := r.DB.GetDerived("version", k)
	if err != nil {
		return "", 0, err
	}
	return decodeVersion(v)
}

// blameParents: the previous versions of the path (live ones only: a
// re-added file's lines are new), then the rename source.
func (r *Repo) blameParents(k object.ID) ([]object.ID, error) {
	path, link, err := r.versionOf(k)
	if err != nil {
		return nil, err
	}
	prev, err := r.previousVersions(pathVersion{path: path, link: link}, true)
	if err != nil {
		return nil, err
	}
	var out []object.ID
	for _, p := range prev {
		if p.dir {
			continue
		}
		id, err := r.registerVersion(p.path, p.link)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	if src, ok, err := r.copySource(path, link); err != nil {
		return nil, err
	} else if ok {
		id, err := r.registerVersion(src.path, src.link)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

// versionContent returns the file content of a path version.
func (r *Repo) versionContent(path string, link uint64) ([]byte, error) {
	cs, err := r.DB.LogID(link)
	if err != nil {
		return nil, err
	}
	root, err := r.RootTree(cs)
	if err != nil {
		return nil, err
	}
	e, err := r.Lookup(root, path)
	if err != nil {
		return nil, err
	}
	if e.Mode == object.ModeDir {
		return nil, fmt.Errorf("%q is a directory", path)
	}
	return r.ReadBlob(e.ID)
}

func (r *Repo) computeBlame(k object.ID, parents []object.ID, vals [][]byte) ([]byte, error) {
	path, link, err := r.versionOf(k)
	if err != nil {
		return nil, err
	}
	content, err := r.versionContent(path, link)
	if err != nil {
		return nil, err
	}
	lines := diff.SplitLines(content)
	attr := make([]blameRun, len(lines))
	for i := range attr {
		attr[i] = blameRun{1, link, i}
	}
	assigned := make([]bool, len(lines))
	for pi, p := range parents {
		pp, pl, err := r.versionOf(p)
		if err != nil {
			return nil, err
		}
		pc, err := r.versionContent(pp, pl)
		if err != nil {
			continue
		}
		pruns, err := decodeBlame(vals[pi])
		if err != nil {
			return nil, err
		}
		plines := expandBlame(pruns)
		for _, m := range diff.Lines(diff.SplitLines(pc), lines) {
			for t := 0; t < m.N; t++ {
				if !assigned[m.B+t] && m.A+t < len(plines) {
					attr[m.B+t] = plines[m.A+t]
					assigned[m.B+t] = true
				}
			}
		}
	}
	return encodeBlame(compressBlame(attr)), nil
}

// Blame attributes each line of path at rev to the changeset that
// introduced it. Results are memoized per file version and computed
// incrementally from the previous version's blame.
func (r *Repo) Blame(rev object.ID, path string) ([]BlameLine, error) {
	v, err := r.lookupVersion(rev, path, true)
	if err != nil {
		return nil, err
	}
	if v.dir {
		return nil, fmt.Errorf("%q is a directory", path)
	}
	k, err := r.registerVersion(v.path, v.link)
	if err != nil {
		return nil, err
	}
	val, err := r.eng.Get(r.blame, k)
	if err != nil {
		return nil, err
	}
	runs, err := decodeBlame(val)
	if err != nil {
		return nil, err
	}
	ids := map[uint64]object.ID{}
	var out []BlameLine
	for _, l := range expandBlame(runs) {
		id, ok := ids[l.seq]
		if !ok {
			if id, err = r.DB.LogID(l.seq); err != nil {
				return nil, err
			}
			ids[l.seq] = id
		}
		out = append(out, BlameLine{id, l.orig + 1})
	}
	return out, nil
}
