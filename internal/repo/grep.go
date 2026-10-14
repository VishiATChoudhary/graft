package repo

import (
	"bytes"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/VishiATChoudhary/graft/internal/diff"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// GrepMatch is one matching line.
type GrepMatch struct {
	Path string `json:"path"`
	Line int    `json:"line"`
	Text string `json:"text"`
}

// GrepOptions controls a search.
type GrepOptions struct {
	Pattern    string
	Fixed      bool // literal string, not a regular expression
	IgnoreCase bool
	Prefix     string                 // only paths under this directory
	Limit      int                    // maximum matches (0: 1000)
	CanRead    func(path string) bool // access filter (nil: all)
	NoIndex    bool                   // scan every file (for testing the index)
}

// Grep searches file contents of a revision on the server. Directories
// whose search filter (trigram.go) shows they cannot contain the pattern's
// required text are skipped; the remaining files are scanned in parallel,
// each tested whole before being split into lines. Results are exactly
// those of a full scan.
func (r *Repo) Grep(root object.ID, opt GrepOptions) ([]GrepMatch, bool, error) {
	if opt.Limit <= 0 {
		opt.Limit = 1000
	}
	pat := opt.Pattern
	if opt.Fixed {
		pat = regexp.QuoteMeta(pat)
	}
	if opt.IgnoreCase {
		pat = "(?i)" + pat
	}
	re, err := regexp.Compile(pat)
	if err != nil {
		return nil, false, err
	}
	var literal []byte
	if opt.Fixed && !opt.IgnoreCase {
		literal = []byte(opt.Pattern)
	}
	type file struct {
		path string
		id   object.ID
	}
	type dir struct {
		tree  object.ID
		files []file
	}
	var dirs []dir
	var files []file
	start, err := r.Lookup(root, opt.Prefix)
	if err != nil {
		return nil, false, err
	}
	var walk func(path string, id object.ID) error
	walk = func(path string, id object.ID) error {
		if opt.CanRead != nil && path != "" && !opt.CanRead(path) {
			return nil
		}
		t, err := store.ReadTree(r.DB, id)
		if err != nil {
			return err
		}
		d := dir{tree: id}
		for _, e := range t.Entries {
			p := join(path, e.Name)
			switch e.Mode {
			case object.ModeDir:
				if err := walk(p, e.ID); err != nil {
					return err
				}
			case object.ModeFile, object.ModeExec:
				d.files = append(d.files, file{p, e.ID})
			}
		}
		if len(d.files) > 0 {
			dirs = append(dirs, d)
		}
		return nil
	}
	prefix := strings.Trim(opt.Prefix, "/")
	switch {
	case start.Mode == object.ModeDir && !start.ID.IsZero():
		if err := walk(prefix, start.ID); err != nil {
			return nil, false, err
		}
	case start.Mode == object.ModeFile || start.Mode == object.ModeExec:
		files = append(files, file{prefix, start.ID})
	}
	// Use the directory filters when the pattern requires some text.
	if tris := queryTrigrams(requiredLiterals(opt.Pattern, opt.Fixed)); len(tris) > 0 && !opt.NoIndex {
		keep := make([]bool, len(dirs))
		var werr error
		var emu sync.Mutex
		var wg sync.WaitGroup
		next := make(chan int, len(dirs))
		for i := range dirs {
			next <- i
		}
		close(next)
		for w := 0; w < runtime.GOMAXPROCS(0); w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range next {
					ids := make([]object.ID, len(dirs[i].files))
					for j, f := range dirs[i].files {
						ids[j] = f.id
					}
					b, err := r.dirBloom(dirs[i].tree, ids)
					if err != nil {
						emu.Lock()
						werr = err
						emu.Unlock()
						continue
					}
					keep[i] = b.mayContain(tris)
				}
			}()
		}
		wg.Wait()
		if werr != nil {
			return nil, false, werr
		}
		for i, d := range dirs {
			if keep[i] {
				files = append(files, d.files...)
			}
		}
	} else {
		for _, d := range dirs {
			files = append(files, d.files...)
		}
	}

	var (
		mu        sync.Mutex
		out       []GrepMatch
		truncated bool
		firstErr  error
		wg        sync.WaitGroup
	)
	work := make(chan file, 256)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range work {
				mu.Lock()
				full := len(out) >= opt.Limit
				mu.Unlock()
				if full {
					continue
				}
				b, err := r.ReadBlob(f.id)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					continue
				}
				if diff.IsBinary(b) {
					continue
				}
				if literal != nil {
					if !bytes.Contains(b, literal) {
						continue
					}
				} else if !re.Match(b) {
					continue
				}
				var found []GrepMatch
				for i, line := range diff.SplitLines(b) {
					if re.Match(line) {
						found = append(found, GrepMatch{f.path, i + 1, strings.TrimRight(string(line), "\r\n")})
					}
				}
				mu.Lock()
				out = append(out, found...)
				mu.Unlock()
			}
		}()
	}
	for _, f := range files {
		work <- f
	}
	close(work)
	wg.Wait()
	if firstErr != nil {
		return nil, false, firstErr
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return out[i].Line < out[j].Line
	})
	if len(out) > opt.Limit {
		out, truncated = out[:opt.Limit], true
	}
	return out, truncated, nil
}
