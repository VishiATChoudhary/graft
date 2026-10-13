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

// Blame runs: blame for a file unode is stored as runs of consecutive lines
// sharing a changeset, with their original line numbers.
type blameRun struct {
	n    int
	cs   object.ID
	orig int // 0-based line number in cs's version of the file
}

func encodeBlame(runs []blameRun) []byte {
	var e object.Encoder
	// Changeset table, then runs referencing it.
	idx := map[object.ID]int{}
	var tbl []object.ID
	for _, r := range runs {
		if _, ok := idx[r.cs]; !ok {
			idx[r.cs] = len(tbl)
			tbl = append(tbl, r.cs)
		}
	}
	e.Uvarint(uint64(len(tbl)))
	for _, id := range tbl {
		e.ID(id)
	}
	e.Uvarint(uint64(len(runs)))
	for _, r := range runs {
		e.Uvarint(uint64(r.n))
		e.Uvarint(uint64(idx[r.cs]))
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
	tbl := make([]object.ID, 0, n)
	for i := uint64(0); i < n && d.Err == nil; i++ {
		tbl = append(tbl, d.ID())
	}
	n = d.Uvarint()
	if n > uint64(len(b)) {
		return nil, object.ErrCorrupt
	}
	runs := make([]blameRun, 0, n)
	for i := uint64(0); i < n && d.Err == nil; i++ {
		cnt, ci, orig := d.Uvarint(), d.Uvarint(), d.Uvarint()
		if ci >= uint64(len(tbl)) {
			return nil, object.ErrCorrupt
		}
		runs = append(runs, blameRun{int(cnt), tbl[ci], int(orig)})
	}
	return runs, d.Err
}

func expandBlame(runs []blameRun) []blameRun {
	var out []blameRun
	for _, r := range runs {
		for k := 0; k < r.n; k++ {
			out = append(out, blameRun{1, r.cs, r.orig + k})
		}
	}
	return out
}

func compressBlame(lines []blameRun) []blameRun {
	var out []blameRun
	for _, l := range lines {
		if n := len(out); n > 0 && out[n-1].cs == l.cs && out[n-1].orig+out[n-1].n == l.orig {
			out[n-1].n++
			continue
		}
		out = append(out, l)
	}
	return out
}

// blameParents: the previous versions of the same path, then the rename
// source. A tombstone ends the chain: a re-added file's lines are new.
func (r *Repo) blameParents(id object.ID) ([]object.ID, error) {
	u, err := r.readUnode(id)
	if err != nil {
		return nil, err
	}
	if u.Dir {
		return nil, fmt.Errorf("blame of a directory")
	}
	if u.Deleted {
		return nil, nil
	}
	var ps []object.ID
	for _, p := range u.Parents {
		pu, err := r.readUnode(p)
		if err != nil {
			return nil, err
		}
		if !pu.Deleted {
			ps = append(ps, p)
		}
	}
	if !u.CopyFrom.IsZero() {
		ps = append(ps, u.CopyFrom)
	}
	return ps, nil
}

func (r *Repo) computeBlame(id object.ID, parents []object.ID, vals [][]byte) ([]byte, error) {
	u, err := r.readUnode(id)
	if err != nil {
		return nil, err
	}
	if u.Deleted {
		return encodeBlame(nil), nil
	}
	content, err := r.ReadBlob(u.Blob)
	if err != nil {
		return nil, err
	}
	lines := diff.SplitLines(content)
	attr := make([]blameRun, len(lines))
	for i := range attr {
		attr[i] = blameRun{1, u.Link, i}
	}
	assigned := make([]bool, len(lines))
	for pi, p := range parents {
		pu, err := r.readUnode(p)
		if err != nil {
			return nil, err
		}
		pc, err := r.ReadBlob(pu.Blob)
		if err != nil {
			return nil, err
		}
		pruns, err := decodeBlame(vals[pi])
		if err != nil {
			return nil, err
		}
		plines := expandBlame(pruns)
		for _, m := range diff.Lines(diff.SplitLines(pc), lines) {
			for k := 0; k < m.N; k++ {
				if !assigned[m.B+k] && m.A+k < len(plines) {
					attr[m.B+k] = plines[m.A+k]
					assigned[m.B+k] = true
				}
			}
		}
	}
	return encodeBlame(compressBlame(attr)), nil
}

// Blame attributes each line of path at rev to the changeset that
// introduced it. Results are memoized per file version, and computed
// incrementally from the previous version's blame.
func (r *Repo) Blame(rev object.ID, path string) ([]BlameLine, error) {
	id, u, err := r.lookupUnode(rev, path, true)
	if err != nil {
		return nil, err
	}
	if u.Dir {
		return nil, fmt.Errorf("%q is a directory", path)
	}
	v, err := r.eng.Get(r.blame, id)
	if err != nil {
		return nil, err
	}
	runs, err := decodeBlame(v)
	if err != nil {
		return nil, err
	}
	var out []BlameLine
	for _, l := range expandBlame(runs) {
		out = append(out, BlameLine{l.cs, l.orig + 1})
	}
	return out, nil
}
