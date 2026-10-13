// Package gitimport ingests a git repository into graft by streaming
// `git fast-export`. fast-export emits each commit's file changes relative
// to its first parent, which is exactly a graft changeset, so no trees need
// to be read from git at all. Blobs are hashed and compressed in parallel;
// changesets are appended in order.
package gitimport

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// Options configures an import.
type Options struct {
	GitDir string   // path to the git repository
	Refs   []string // git refs to import (default: HEAD)
	// RefMap maps git ref names to graft ref names. Unmapped branch refs
	// "refs/heads/x" become "x"; tags become "tags/x".
	RefMap   map[string]string
	Progress func(Stats)
}

// Stats reports import progress.
type Stats struct {
	Blobs, Changesets int
	BlobBytes         int64
	Elapsed           time.Duration
}

type future struct {
	done chan struct{}
	id   object.ID
	err  error
}

// Import runs the import and returns final stats.
func Import(ctx context.Context, r *repo.Repo, opt Options) (*Stats, error) {
	refs := opt.Refs
	if len(refs) == 0 {
		refs = []string{"HEAD"}
	}
	args := append([]string{"-C", opt.GitDir, "fast-export", "--reencode=yes", "--signed-tags=strip",
		"--tag-of-filtered-object=drop", "--show-original-ids", "--use-done-feature"}, refs...)
	cmd := exec.CommandContext(ctx, "git", args...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	imp := &importer{r: r, opt: opt, start: time.Now(), marks: map[string]*future{}, commits: map[string]object.ID{},
		refs: map[string]object.ID{}, work: make(chan blobWork, 1024)}
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		imp.wg.Add(1)
		go imp.blobWorker()
	}
	perr := imp.parse(bufio.NewReaderSize(out, 4<<20))
	close(imp.work)
	imp.wg.Wait()
	if perr != nil {
		cmd.Process.Kill()
		cmd.Wait()
		return &imp.stats, perr
	}
	if err := cmd.Wait(); err != nil {
		return &imp.stats, fmt.Errorf("git fast-export: %v: %s", err, stderr.String())
	}
	for gitRef, id := range imp.refs {
		name := mapRef(gitRef, opt.RefMap)
		if name == "" {
			continue
		}
		cur, err := r.Ref(name)
		if err == object.ErrNotFound {
			cur = object.Zero
		} else if err != nil {
			return &imp.stats, err
		}
		if err := r.UpdateRef(name, cur, id); err != nil {
			return &imp.stats, err
		}
	}
	if err := r.DB.Flush(); err != nil {
		return &imp.stats, err
	}
	imp.stats.Elapsed = time.Since(imp.start)
	return &imp.stats, nil
}

func mapRef(gitRef string, m map[string]string) string {
	if v, ok := m[gitRef]; ok {
		return v
	}
	switch {
	case strings.HasPrefix(gitRef, "refs/heads/"):
		return strings.TrimPrefix(gitRef, "refs/heads/")
	case strings.HasPrefix(gitRef, "refs/tags/"):
		return "tags/" + strings.TrimPrefix(gitRef, "refs/tags/")
	}
	return ""
}

type blobWork struct {
	f    *future
	data []byte
}

type importer struct {
	r       *repo.Repo
	opt     Options
	start   time.Time
	stats   Stats
	mu      sync.Mutex
	marks   map[string]*future   // blob marks
	commits map[string]object.ID // commit marks -> changeset
	refs    map[string]object.ID
	work    chan blobWork
	wg      sync.WaitGroup
}

func (imp *importer) blobWorker() {
	defer imp.wg.Done()
	for w := range imp.work {
		w.f.id, w.f.err = store.WritePayload(imp.r.DB, object.KindBlob, w.data)
		close(w.f.done)
	}
}

func readLine(br *bufio.Reader) (string, error) {
	l, err := br.ReadString('\n')
	if err != nil {
		if err == io.EOF && l != "" {
			return l, nil
		}
		return "", err
	}
	return strings.TrimSuffix(l, "\n"), nil
}

func readData(br *bufio.Reader, header string) ([]byte, error) {
	n, err := strconv.Atoi(strings.TrimPrefix(header, "data "))
	if err != nil || !strings.HasPrefix(header, "data ") || strings.HasPrefix(header, "data <<") {
		return nil, fmt.Errorf("unsupported data header %q", header)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(br, b); err != nil {
		return nil, err
	}
	// Optional LF after data.
	if c, err := br.Peek(1); err == nil && c[0] == '\n' {
		br.ReadByte()
	}
	return b, nil
}

func (imp *importer) parse(br *bufio.Reader) error {
	var pending string
	next := func() (string, error) {
		if pending != "" {
			l := pending
			pending = ""
			return l, nil
		}
		return readLine(br)
	}
	for {
		l, err := next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch {
		case l == "" || strings.HasPrefix(l, "feature ") || strings.HasPrefix(l, "progress "):
		case l == "done":
			return nil
		case l == "blob":
			if err := imp.parseBlob(br); err != nil {
				return err
			}
		case strings.HasPrefix(l, "commit "):
			p, err := imp.parseCommit(br, strings.TrimPrefix(l, "commit "))
			if err != nil {
				return err
			}
			pending = p
		case strings.HasPrefix(l, "reset "):
			ref := strings.TrimPrefix(l, "reset ")
			nl, err := readLine(br)
			if err != nil {
				return err
			}
			if strings.HasPrefix(nl, "from ") {
				if id, ok := imp.commits[strings.TrimPrefix(nl, "from ")]; ok {
					imp.refs[ref] = id
				}
			} else {
				pending = nl
			}
		case strings.HasPrefix(l, "tag "):
			if err := imp.parseTag(br, strings.TrimPrefix(l, "tag ")); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected fast-export line %q", l)
		}
	}
}

func (imp *importer) parseBlob(br *bufio.Reader) error {
	var mark string
	for {
		l, err := readLine(br)
		if err != nil {
			return err
		}
		switch {
		case strings.HasPrefix(l, "mark "):
			mark = strings.TrimPrefix(l, "mark ")
		case strings.HasPrefix(l, "original-oid "):
		case strings.HasPrefix(l, "data "):
			data, err := readData(br, l)
			if err != nil {
				return err
			}
			f := &future{done: make(chan struct{})}
			imp.marks[mark] = f
			imp.stats.Blobs++
			imp.stats.BlobBytes += int64(len(data))
			imp.work <- blobWork{f, data}
			return nil
		default:
			return fmt.Errorf("unexpected blob line %q", l)
		}
	}
}

func (imp *importer) parseTag(br *bufio.Reader, name string) error {
	var target object.ID
	for {
		l, err := readLine(br)
		if err != nil {
			return err
		}
		switch {
		case strings.HasPrefix(l, "from "):
			target = imp.commits[strings.TrimPrefix(l, "from ")]
		case strings.HasPrefix(l, "data "):
			if _, err := readData(br, l); err != nil {
				return err
			}
			if !target.IsZero() {
				imp.refs["refs/tags/"+name] = target
			}
			return nil
		}
	}
}

// parseIdent parses "Name <email> 1700000000 +0100".
func parseIdent(s string) (who string, when int64, tz int32) {
	i := strings.LastIndexByte(s, '>')
	if i < 0 {
		return s, 0, 0
	}
	who = s[:i+1]
	f := strings.Fields(s[i+1:])
	if len(f) >= 1 {
		when, _ = strconv.ParseInt(f[0], 10, 64)
	}
	if len(f) >= 2 && len(f[1]) == 5 {
		h, _ := strconv.Atoi(f[1][1:3])
		m, _ := strconv.Atoi(f[1][3:5])
		tz = int32(h*60 + m)
		if f[1][0] == '-' {
			tz = -tz
		}
	}
	return
}

// unquote decodes a fast-export path, which may be C-style quoted.
func unquote(p string) (string, error) {
	if !strings.HasPrefix(p, `"`) {
		return p, nil
	}
	s, err := strconv.Unquote(p)
	if err == nil {
		return s, nil
	}
	// Git uses octal escapes for raw bytes, which strconv accepts only as
	// valid UTF-8 in some cases; decode manually.
	var out []byte
	body := p[1 : len(p)-1]
	for i := 0; i < len(body); i++ {
		c := body[i]
		if c != '\\' || i+1 >= len(body) {
			out = append(out, c)
			continue
		}
		i++
		switch e := body[i]; e {
		case 'n':
			out = append(out, '\n')
		case 't':
			out = append(out, '\t')
		case '"', '\\':
			out = append(out, e)
		case 'a':
			out = append(out, 7)
		case 'b':
			out = append(out, 8)
		case 'f':
			out = append(out, 12)
		case 'r':
			out = append(out, 13)
		case 'v':
			out = append(out, 11)
		default:
			if i+2 < len(body) {
				v, err := strconv.ParseUint(body[i:i+3], 8, 8)
				if err != nil {
					return "", fmt.Errorf("bad quoted path %s", p)
				}
				out = append(out, byte(v))
				i += 2
			}
		}
	}
	return string(out), nil
}

func gitMode(m string) (object.Mode, error) {
	switch m {
	case "100644", "644":
		return object.ModeFile, nil
	case "100755", "755":
		return object.ModeExec, nil
	case "120000":
		return object.ModeSymlink, nil
	case "160000":
		return object.ModeGitlink, nil
	}
	return 0, fmt.Errorf("unsupported mode %s", m)
}

type pendingChange struct {
	path string
	mode object.Mode
	f    *future
	del  bool
}

// parseCommit reads one commit and appends it. It returns any line it read
// past the end of the commit.
func (imp *importer) parseCommit(br *bufio.Reader, ref string) (string, error) {
	cs := &object.Changeset{}
	var mark string
	var committer string
	var author string
	var changes []pendingChange
	deleteAll := false
	var leftover string
	for {
		l, err := readLine(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		switch {
		case strings.HasPrefix(l, "mark "):
			mark = strings.TrimPrefix(l, "mark ")
		case strings.HasPrefix(l, "original-oid "):
			cs.Origin = "git:" + strings.TrimPrefix(l, "original-oid ")
		case strings.HasPrefix(l, "author "):
			author = strings.TrimPrefix(l, "author ")
		case strings.HasPrefix(l, "committer "):
			committer = strings.TrimPrefix(l, "committer ")
		case strings.HasPrefix(l, "encoding "):
		case strings.HasPrefix(l, "data "):
			msg, err := readData(br, l)
			if err != nil {
				return "", err
			}
			cs.Message = string(msg)
		case strings.HasPrefix(l, "from "), strings.HasPrefix(l, "merge "):
			pm := l[strings.IndexByte(l, ' ')+1:]
			id, ok := imp.commits[pm]
			if !ok {
				return "", fmt.Errorf("commit references unknown parent %s", pm)
			}
			cs.Parents = append(cs.Parents, id)
		case strings.HasPrefix(l, "M "):
			parts := strings.SplitN(l, " ", 4)
			if len(parts) != 4 {
				return "", fmt.Errorf("bad filemodify %q", l)
			}
			mode, err := gitMode(parts[1])
			if err != nil {
				return "", err
			}
			p, err := unquote(parts[3])
			if err != nil {
				return "", err
			}
			pc := pendingChange{path: p, mode: mode}
			if mode == object.ModeGitlink {
				f := &future{done: make(chan struct{})}
				f.id, f.err = store.WritePayload(imp.r.DB, object.KindBlob, []byte(parts[2]))
				close(f.done)
				pc.f = f
			} else {
				f, ok := imp.marks[parts[2]]
				if !ok {
					return "", fmt.Errorf("unknown blob %s (inline data unsupported)", parts[2])
				}
				pc.f = f
			}
			changes = append(changes, pc)
		case strings.HasPrefix(l, "D "):
			p, err := unquote(l[2:])
			if err != nil {
				return "", err
			}
			changes = append(changes, pendingChange{path: p, del: true})
		case l == "deleteall":
			deleteAll = true
			changes = nil
		case l == "":
			goto done
		default:
			leftover = l
			goto done
		}
	}
done:
	who, when, tz := parseIdent(author)
	if author == "" {
		who, when, tz = parseIdent(committer)
	}
	cs.Author, cs.Time, cs.TZ = who, when, tz
	if deleteAll && len(cs.Parents) > 0 {
		root, err := imp.r.RootTree(cs.Parents[0])
		if err != nil {
			return "", err
		}
		ents, err := imp.r.ListTree(root, "")
		if err != nil {
			return "", err
		}
		var dels []pendingChange
		for _, e := range ents {
			dels = append(dels, pendingChange{path: e.Name, del: true})
		}
		changes = append(dels, changes...)
	}
	for _, c := range changes {
		if c.del {
			cs.Changes = append(cs.Changes, object.Change{Path: c.path})
			continue
		}
		<-c.f.done
		if c.f.err != nil {
			return "", c.f.err
		}
		cs.Changes = append(cs.Changes, object.Change{Path: c.path, Mode: c.mode, Blob: c.f.id})
	}
	id, err := imp.r.Create(cs)
	if err != nil {
		return "", fmt.Errorf("commit %s: %w", cs.Origin, err)
	}
	imp.commits[mark] = id
	imp.refs[ref] = id
	imp.stats.Changesets++
	if imp.opt.Progress != nil && imp.stats.Changesets%1000 == 0 {
		imp.stats.Elapsed = time.Since(imp.start)
		imp.opt.Progress(imp.stats)
	}
	return leftover, nil
}
