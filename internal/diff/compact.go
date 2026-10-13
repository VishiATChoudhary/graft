package diff

// Change compaction, ported from git's xdiff (xdl_change_compact and the
// indent heuristic, on by default since git 2.14). A group of changed lines
// can often slide up or down without changing the diff's meaning (when the
// line before the group equals its last line, and so on). Git slides each
// group to line up with the other file's changes, or else to the position
// its indent heuristic scores best. Doing the same makes graft's diffs and
// blame agree with git's choices.

type cfile struct {
	recs []int    // interned line ids
	text [][]byte // line contents (for indentation)
	rchg []bool   // len(recs)+2: rchg[i+1] is line i; sentinels at both ends
}

func (f *cfile) chg(i int) bool      { return f.rchg[i+1] }
func (f *cfile) set(i int, v bool)   { f.rchg[i+1] = v }
func (f *cfile) nrec() int           { return len(f.recs) }
func (f *cfile) match(i, j int) bool { return f.recs[i] == f.recs[j] }

type group struct{ start, end int }

func groupInit(f *cfile) group {
	g := group{}
	for f.chg(g.end) {
		g.end++
	}
	return g
}

func groupNext(f *cfile, g *group) bool {
	if g.end == f.nrec() {
		return false
	}
	g.start = g.end + 1
	for g.end = g.start; f.chg(g.end); g.end++ {
	}
	return true
}

func groupPrevious(f *cfile, g *group) bool {
	if g.start == 0 {
		return false
	}
	g.end = g.start - 1
	for g.start = g.end; f.chg(g.start - 1); g.start-- {
	}
	return true
}

func groupSlideDown(f *cfile, g *group) bool {
	if g.end < f.nrec() && f.match(g.start, g.end) {
		f.set(g.start, false)
		g.start++
		f.set(g.end, true)
		g.end++
		for f.chg(g.end) {
			g.end++
		}
		return true
	}
	return false
}

func groupSlideUp(f *cfile, g *group) bool {
	if g.start > 0 && f.match(g.start-1, g.end-1) {
		g.start--
		f.set(g.start, true)
		g.end--
		f.set(g.end, false)
		for f.chg(g.start - 1) {
			g.start--
		}
		return true
	}
	return false
}

const (
	maxIndent                       = 200
	maxBlanks                       = 20
	startOfFilePenalty              = 1
	endOfFilePenalty                = 21
	totalBlankWeight                = -30
	postBlankWeight                 = 6
	relativeIndentPenalty           = -4
	relativeIndentWithBlankPenalty  = 10
	relativeOutdentPenalty          = 24
	relativeOutdentWithBlankPenalty = 17
	relativeDedentPenalty           = 23
	relativeDedentWithBlankPenalty  = 17
	indentWeight                    = 60
	indentHeuristicMaxSliding       = 100
)

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r'
}

// getIndent returns the indentation width of a line, or -1 if it is blank.
func getIndent(line []byte) int {
	ret := 0
	for _, c := range line {
		if !isSpace(c) {
			return ret
		} else if c == ' ' {
			ret++
		} else if c == '\t' {
			ret += 8 - ret%8
		}
		if ret >= maxIndent {
			return maxIndent
		}
	}
	return -1
}

type splitMeasurement struct {
	endOfFile                                          bool
	indent, preBlank, preIndent, postBlank, postIndent int
}

func measureSplit(f *cfile, split int) splitMeasurement {
	var m splitMeasurement
	if split >= f.nrec() {
		m.endOfFile, m.indent = true, -1
	} else {
		m.indent = getIndent(f.text[split])
	}
	m.preIndent = -1
	for i := split - 1; i >= 0; i-- {
		m.preIndent = getIndent(f.text[i])
		if m.preIndent != -1 {
			break
		}
		m.preBlank++
		if m.preBlank == maxBlanks {
			m.preIndent = 0
			break
		}
	}
	m.postIndent = -1
	for i := split + 1; i < f.nrec(); i++ {
		m.postIndent = getIndent(f.text[i])
		if m.postIndent != -1 {
			break
		}
		m.postBlank++
		if m.postBlank == maxBlanks {
			m.postIndent = 0
			break
		}
	}
	return m
}

type splitScore struct{ effectiveIndent, penalty int }

func scoreAddSplit(m splitMeasurement, s *splitScore) {
	if m.preIndent == -1 && m.preBlank == 0 {
		s.penalty += startOfFilePenalty
	}
	if m.endOfFile {
		s.penalty += endOfFilePenalty
	}
	postBlank := 0
	if m.indent == -1 {
		postBlank = 1 + m.postBlank
	}
	totalBlank := m.preBlank + postBlank
	s.penalty += totalBlankWeight * totalBlank
	s.penalty += postBlankWeight * postBlank
	indent := m.indent
	if indent == -1 {
		indent = m.postIndent
	}
	anyBlanks := totalBlank != 0
	s.effectiveIndent += indent
	switch {
	case indent == -1, m.preIndent == -1, indent == m.preIndent:
	case indent > m.preIndent:
		if anyBlanks {
			s.penalty += relativeIndentWithBlankPenalty
		} else {
			s.penalty += relativeIndentPenalty
		}
	default:
		if m.postIndent != -1 && m.postIndent > indent {
			if anyBlanks {
				s.penalty += relativeOutdentWithBlankPenalty
			} else {
				s.penalty += relativeOutdentPenalty
			}
		} else {
			if anyBlanks {
				s.penalty += relativeDedentWithBlankPenalty
			} else {
				s.penalty += relativeDedentPenalty
			}
		}
	}
}

func scoreCmp(a, b splitScore) int {
	ci := 0
	if a.effectiveIndent > b.effectiveIndent {
		ci = 1
	} else if a.effectiveIndent < b.effectiveIndent {
		ci = -1
	}
	return indentWeight*ci + (a.penalty - b.penalty)
}

// changeCompact slides the change groups of f (aligned with fo's groups).
func changeCompact(f, fo *cfile) {
	g, gOther := groupInit(f), groupInit(fo)
	for {
		if g.end != g.start {
			var groupsize, earliestEnd, endMatchingOther int
			for {
				groupsize = g.end - g.start
				endMatchingOther = -1
				for groupSlideUp(f, &g) {
					groupPrevious(fo, &gOther)
				}
				earliestEnd = g.end
				if gOther.end > gOther.start {
					endMatchingOther = g.end
				}
				for groupSlideDown(f, &g) {
					groupNext(fo, &gOther)
					if gOther.end > gOther.start {
						endMatchingOther = g.end
					}
				}
				if groupsize == g.end-g.start {
					break
				}
			}
			switch {
			case g.end == earliestEnd:
			case endMatchingOther != -1:
				for gOther.end == gOther.start {
					groupSlideUp(f, &g)
					groupPrevious(fo, &gOther)
				}
			default:
				shift := earliestEnd
				if g.end-groupsize-1 > shift {
					shift = g.end - groupsize - 1
				}
				if g.end-indentHeuristicMaxSliding > shift {
					shift = g.end - indentHeuristicMaxSliding
				}
				bestShift := -1
				var best splitScore
				for ; shift <= g.end; shift++ {
					var score splitScore
					scoreAddSplit(measureSplit(f, shift), &score)
					scoreAddSplit(measureSplit(f, shift-groupsize), &score)
					if bestShift == -1 || scoreCmp(score, best) <= 0 {
						best, bestShift = score, shift
					}
				}
				for g.end > bestShift {
					groupSlideUp(f, &g)
					groupPrevious(fo, &gOther)
				}
			}
		}
		if !groupNext(f, &g) {
			break
		}
		groupNext(fo, &gOther)
	}
}

// compactChanges applies git's compaction to change flags and returns the
// resulting match runs.
func compactChanges(a, b []int, ta, tb [][]byte, ca, cb []bool) []Match {
	fa := &cfile{recs: a, text: ta, rchg: make([]bool, len(a)+2)}
	fb := &cfile{recs: b, text: tb, rchg: make([]bool, len(b)+2)}
	copy(fa.rchg[1:], ca)
	copy(fb.rchg[1:], cb)
	changeCompact(fa, fb)
	changeCompact(fb, fa)
	return matchesFromChanges(fa.rchg[1:len(a)+1], fb.rchg[1:len(b)+1])
}
