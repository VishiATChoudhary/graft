package diff

import "math"

// A port of git's xdiff Myers implementation (xprepare.c and xdiffi.c):
// common prefix/suffix trimming, discarding of lines that cannot match or
// match too often (xdl_cleanup_records), and divide-and-conquer Myers with
// git's tie-breaking and cost heuristics (xdl_split). Following git's
// algorithm, rather than any minimal diff, is what makes graft's blame pick
// the same lines as git blame when several alignments are equally short.

const (
	xdlMaxEqLimit    = 1024
	xdlSimscanWindow = 100
	xdlKpdisRun      = 4
	xdlMaxCostMin    = 256
	xdlHeurMinCost   = 256
	xdlSnakeCnt      = 20
	xdlKHeur         = 4
	xdlLineMax       = math.MaxInt
)

func bogosqrt(n int) int {
	i := 1
	for ; n > 0; n >>= 2 {
		i <<= 1
	}
	return i
}

type xfile struct {
	ha     []int  // all records
	rchg   []bool // changed flags per record
	rindex []int  // reduced index -> record index
	rha    []int  // reduced hashes
}

func cleanMMatch(dis []byte, i, s, e int) bool {
	if i-s > xdlSimscanWindow {
		s = i - xdlSimscanWindow
	}
	if e-i > xdlSimscanWindow {
		e = i + xdlSimscanWindow
	}
	rdis0, rpdis0 := 0, 1
	for r := 1; i-r >= s; r++ {
		if dis[i-r] == 0 {
			rdis0++
		} else if dis[i-r] == 2 {
			rpdis0++
		} else {
			break
		}
	}
	if rdis0 == 0 {
		return false
	}
	rdis1, rpdis1 := 0, 1
	for r := 1; i+r <= e; r++ {
		if dis[i+r] == 0 {
			rdis1++
		} else if dis[i+r] == 2 {
			rpdis1++
		} else {
			break
		}
	}
	if rdis1 == 0 {
		return false
	}
	rdis1 += rdis0
	rpdis1 += rpdis0
	return rpdis1*xdlKpdisRun < rpdis1+rdis1
}

// xdiff computes change flags for a and b the way git does.
func xdiff(a, b []int) (rchgA, rchgB []bool) {
	f1 := &xfile{ha: a, rchg: make([]bool, len(a))}
	f2 := &xfile{ha: b, rchg: make([]bool, len(b))}

	// xdl_trim_ends
	lim := min(len(a), len(b))
	i := 0
	for i < lim && a[i] == b[i] {
		i++
	}
	dstart := i
	j := 0
	for lim -= i; j < lim && a[len(a)-1-j] == b[len(b)-1-j]; j++ {
	}
	dend1, dend2 := len(a)-j-1, len(b)-j-1

	// xdl_cleanup_records
	count1 := map[int]int{}
	count2 := map[int]int{}
	for _, h := range a {
		count1[h]++
	}
	for _, h := range b {
		count2[h]++
	}
	dis1 := make([]byte, len(a)+1)
	dis2 := make([]byte, len(b)+1)
	mlim := min(bogosqrt(len(a)), xdlMaxEqLimit)
	for i := dstart; i <= dend1; i++ {
		nm := count2[a[i]]
		switch {
		case nm == 0:
			dis1[i] = 0
		case nm >= mlim:
			dis1[i] = 2
		default:
			dis1[i] = 1
		}
	}
	mlim = min(bogosqrt(len(b)), xdlMaxEqLimit)
	for i := dstart; i <= dend2; i++ {
		nm := count1[b[i]]
		switch {
		case nm == 0:
			dis2[i] = 0
		case nm >= mlim:
			dis2[i] = 2
		default:
			dis2[i] = 1
		}
	}
	for i := dstart; i <= dend1; i++ {
		if dis1[i] == 1 || (dis1[i] == 2 && !cleanMMatch(dis1, i, dstart, dend1)) {
			f1.rindex = append(f1.rindex, i)
			f1.rha = append(f1.rha, a[i])
		} else {
			f1.rchg[i] = true
		}
	}
	for i := dstart; i <= dend2; i++ {
		if dis2[i] == 1 || (dis2[i] == 2 && !cleanMMatch(dis2, i, dstart, dend2)) {
			f2.rindex = append(f2.rindex, i)
			f2.rha = append(f2.rha, b[i])
		} else {
			f2.rchg[i] = true
		}
	}

	// xdl_do_diff
	n1, n2 := len(f1.rha), len(f2.rha)
	ndiags := n1 + n2 + 3
	kvd := make([]int, 2*ndiags+2)
	x := &xenv{
		kvd:   kvd,
		fbase: n2 + 1,
		bbase: ndiags + n2 + 1,
	}
	x.mxcost = max(bogosqrt(ndiags), xdlMaxCostMin)
	x.recsCmp(f1, 0, n1, f2, 0, n2, false)
	return f1.rchg, f2.rchg
}

type xenv struct {
	kvd          []int
	fbase, bbase int
	mxcost       int
}

func (x *xenv) kf(d int) *int { return &x.kvd[x.fbase+d] }
func (x *xenv) kb(d int) *int { return &x.kvd[x.bbase+d] }

func (x *xenv) recsCmp(f1 *xfile, off1, lim1 int, f2 *xfile, off2, lim2 int, needMin bool) {
	ha1, ha2 := f1.rha, f2.rha
	for off1 < lim1 && off2 < lim2 && ha1[off1] == ha2[off2] {
		off1++
		off2++
	}
	for off1 < lim1 && off2 < lim2 && ha1[lim1-1] == ha2[lim2-1] {
		lim1--
		lim2--
	}
	switch {
	case off1 == lim1:
		for ; off2 < lim2; off2++ {
			f2.rchg[f2.rindex[off2]] = true
		}
	case off2 == lim2:
		for ; off1 < lim1; off1++ {
			f1.rchg[f1.rindex[off1]] = true
		}
	default:
		s := x.split(ha1, off1, lim1, ha2, off2, lim2, needMin)
		x.recsCmp(f1, off1, s.i1, f2, off2, s.i2, s.minLo)
		x.recsCmp(f1, s.i1, lim1, f2, s.i2, lim2, s.minHi)
	}
}

type xsplit struct {
	i1, i2       int
	minLo, minHi bool
}

func (x *xenv) split(ha1 []int, off1, lim1 int, ha2 []int, off2, lim2 int, needMin bool) xsplit {
	dmin, dmax := off1-lim2, lim1-off2
	fmid, bmid := off1-off2, lim1-lim2
	odd := (fmid-bmid)&1 != 0
	fmin, fmax := fmid, fmid
	bmin, bmax := bmid, bmid
	*x.kf(fmid) = off1
	*x.kb(bmid) = lim1
	for ec := 1; ; ec++ {
		gotSnake := false
		if fmin > dmin {
			fmin--
			*x.kf(fmin - 1) = -1
		} else {
			fmin++
		}
		if fmax < dmax {
			fmax++
			*x.kf(fmax + 1) = -1
		} else {
			fmax--
		}
		for d := fmax; d >= fmin; d -= 2 {
			var i1 int
			if *x.kf(d - 1) >= *x.kf(d + 1) {
				i1 = *x.kf(d - 1) + 1
			} else {
				i1 = *x.kf(d + 1)
			}
			prev1 := i1
			i2 := i1 - d
			for i1 < lim1 && i2 < lim2 && ha1[i1] == ha2[i2] {
				i1++
				i2++
			}
			if i1-prev1 > xdlSnakeCnt {
				gotSnake = true
			}
			*x.kf(d) = i1
			if odd && bmin <= d && d <= bmax && *x.kb(d) <= i1 {
				return xsplit{i1, i2, true, true}
			}
		}
		if bmin > dmin {
			bmin--
			*x.kb(bmin - 1) = xdlLineMax
		} else {
			bmin++
		}
		if bmax < dmax {
			bmax++
			*x.kb(bmax + 1) = xdlLineMax
		} else {
			bmax--
		}
		for d := bmax; d >= bmin; d -= 2 {
			var i1 int
			if *x.kb(d - 1) < *x.kb(d + 1) {
				i1 = *x.kb(d - 1)
			} else {
				i1 = *x.kb(d + 1) - 1
			}
			prev1 := i1
			i2 := i1 - d
			for i1 > off1 && i2 > off2 && ha1[i1-1] == ha2[i2-1] {
				i1--
				i2--
			}
			if prev1-i1 > xdlSnakeCnt {
				gotSnake = true
			}
			*x.kb(d) = i1
			if !odd && fmin <= d && d <= fmax && i1 <= *x.kf(d) {
				return xsplit{i1, i2, true, true}
			}
		}
		if needMin {
			continue
		}
		if gotSnake && ec > xdlHeurMinCost {
			best := 0
			var s xsplit
			for d := fmax; d >= fmin; d -= 2 {
				dd := d - fmid
				if dd < 0 {
					dd = -dd
				}
				i1 := *x.kf(d)
				i2 := i1 - d
				v := (i1 - off1) + (i2 - off2) - dd
				if v > xdlKHeur*ec && v > best && off1+xdlSnakeCnt <= i1 && i1 < lim1 && off2+xdlSnakeCnt <= i2 && i2 < lim2 {
					for k := 1; ha1[i1-k] == ha2[i2-k]; k++ {
						if k == xdlSnakeCnt {
							best = v
							s = xsplit{i1, i2, true, false}
							break
						}
					}
				}
			}
			if best > 0 {
				return s
			}
			best = 0
			for d := bmax; d >= bmin; d -= 2 {
				dd := d - bmid
				if dd < 0 {
					dd = -dd
				}
				i1 := *x.kb(d)
				i2 := i1 - d
				v := (lim1 - i1) + (lim2 - i2) - dd
				if v > xdlKHeur*ec && v > best && off1 < i1 && i1 <= lim1-xdlSnakeCnt && off2 < i2 && i2 <= lim2-xdlSnakeCnt {
					for k := 0; ha1[i1+k] == ha2[i2+k]; k++ {
						if k == xdlSnakeCnt-1 {
							best = v
							s = xsplit{i1, i2, false, true}
							break
						}
					}
				}
			}
			if best > 0 {
				return s
			}
		}
		if ec >= x.mxcost {
			fbest, fbest1 := -1, -1
			for d := fmax; d >= fmin; d -= 2 {
				i1 := min(*x.kf(d), lim1)
				i2 := i1 - d
				if lim2 < i2 {
					i1, i2 = lim2+d, lim2
				}
				if fbest < i1+i2 {
					fbest, fbest1 = i1+i2, i1
				}
			}
			bbest, bbest1 := xdlLineMax, xdlLineMax
			for d := bmax; d >= bmin; d -= 2 {
				i1 := max(off1, *x.kb(d))
				i2 := i1 - d
				if i2 < off2 {
					i1, i2 = off2+d, off2
				}
				if i1+i2 < bbest {
					bbest, bbest1 = i1+i2, i1
				}
			}
			if (lim1+lim2)-bbest < fbest-(off1+off2) {
				return xsplit{fbest1, fbest - fbest1, true, false}
			}
			return xsplit{bbest1, bbest - bbest1, false, true}
		}
	}
}

// matchesFromChanges converts change flags into match runs.
func matchesFromChanges(ca, cb []bool) []Match {
	var out []Match
	i, j := 0, 0
	for i < len(ca) || j < len(cb) {
		switch {
		case i < len(ca) && ca[i]:
			i++
		case j < len(cb) && cb[j]:
			j++
		case i < len(ca) && j < len(cb):
			if n := len(out); n > 0 && out[n-1].A+out[n-1].N == i && out[n-1].B+out[n-1].N == j {
				out[n-1].N++
			} else {
				out = append(out, Match{i, j, 1})
			}
			i++
			j++
		default:
			return out
		}
	}
	return out
}
