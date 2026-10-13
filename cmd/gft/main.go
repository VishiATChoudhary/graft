// Command gft is graft's single binary: the client, the server tiers
// (server, regional cache, node daemon), and admin tools.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/VishiATChoudhary/graft/internal/api"
	"github.com/VishiATChoudhary/graft/internal/client"
	"github.com/VishiATChoudhary/graft/internal/diff"
	"github.com/VishiATChoudhary/graft/internal/gitimport"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
	"github.com/VishiATChoudhary/graft/internal/store"
	"github.com/VishiATChoudhary/graft/internal/vfs"
)

const usage = `gft: source control for agent-scale monorepos

Working copy:
  gft clone <server> <dir> [--sparse dir]... [--objects endpoint] [--rev main]
  gft mount <server> <dir> [--rev R]  virtual checkout: files fetched on first read, writes go to an
                                     overlay; status/commit/land/checkout work inside it [--rdonly]
  gft checkout <rev> [--force]       move the working copy (only changed files are touched)
  gft sparse list|add|set <dir>...   change which directories are materialized
  gft watch [--background|--stop]    watch the working copy so status only checks what changed
  gft status                         local modifications
  gft diff [path...]                 local modifications as a patch
  gft commit -m <msg> [path...]      create a changeset on the server (nothing to push)
  gft land [--ref main]              rebase server-side if needed, then CAS the ref
  gft review [rev] [--ref main]      which approvals a change needs (comment-only/docs changes can be exempt)
  gft approve <rev>                  approve a change
  gft rebase <onto>                  rebase the current changeset server-side

Repository queries (served by the server; no local history needed):
  gft log [--rev R] [-n N] [--first-parent] [--follow] [path]
  gft blame [--rev R] <path>
  gft show [rev]
  gft diff --from A --to B [--stat] [path...]
  gft merge-base <a> <b>
  gft is-ancestor <a> <b>
  gft ls [--rev R] [path]
  gft cat [--rev R] <path>
  gft find [--rev R] [--in dir] <substring>                 search paths
  gft grep [--rev R] [--in dir] [-i] [-F] [-n N] <regexp>   search file contents on the server
  gft refs
  gft write --parent R [--land REF] -m <msg> path=@file|path=-delete ...   commit without a checkout

Infrastructure:
  gft serve --tier server --data <dir> [--listen addr]                      single node
  gft serve --tier store  --data <dir> --nodes <u0,u1,..> --shard <i>        storage shard
  gft serve --tier server --store <u0,u1,..> [--listen addr]                stateless API server
  gft serve --tier cache  --upstream <server> [--mem 8G] [--listen addr]
  gft serve --tier daemon --upstream <cache> [--listen unix:///run/gft.sock] [--disk 20G] [--seed file]
  gft snapshot --server <url> --rev main --out <file>
  gft import --data <dir> --git <repo> [--ref refs/heads/main]...
  gft info [--server url]

Environment: GFT_SERVER (default server for repository queries outside a working copy).
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	if prof := os.Getenv("GFT_CPUPROFILE"); prof != "" {
		f, perr := os.Create(prof)
		if perr == nil {
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}
	}
	var err error
	switch cmd {
	case "serve":
		err = cmdServe(args)
	case "snapshot":
		err = cmdSnapshot(args)
	case "import":
		err = cmdImport(args)
	case "clone":
		err = cmdClone(args)
	case "mount":
		err = cmdMount(args)
	case "watch":
		err = cmdWatch(args)
	case "checkout", "co":
		err = cmdCheckout(args)
	case "sparse":
		err = cmdSparse(args)
	case "status", "st":
		err = cmdStatus(args)
	case "commit", "ci":
		err = cmdCommit(args)
	case "land":
		err = cmdLand(args)
	case "rebase":
		err = cmdRebase(args)
	case "log":
		err = cmdLog(args)
	case "blame":
		err = cmdBlame(args)
	case "show":
		err = cmdShow(args)
	case "diff":
		err = cmdDiff(args)
	case "merge-base":
		err = cmdMergeBase(args)
	case "is-ancestor":
		err = cmdIsAncestor(args)
	case "ls":
		err = cmdLs(args)
	case "cat":
		err = cmdCat(args)
	case "find":
		err = cmdFind(args)
	case "grep":
		err = cmdGrep(args)
	case "approve":
		err = cmdApprove(args)
	case "review":
		err = cmdReview(args)
	case "refs":
		err = cmdRefs(args)
	case "write":
		err = cmdWrite(args)
	case "info":
		err = cmdInfo(args)
	case "help", "-h", "--help":
		fmt.Print(usage)
	case "version":
		fmt.Println("gft", version)
	default:
		fmt.Fprintf(os.Stderr, "gft: unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}
	if err != nil {
		var ae *api.APIError
		if errors.As(err, &ae) && len(ae.Conflicts) > 0 {
			fmt.Fprintf(os.Stderr, "gft: conflict in:\n")
			for _, p := range ae.Conflicts {
				fmt.Fprintf(os.Stderr, "  %s\n", p)
			}
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "gft: %v\n", err)
		pprof.StopCPUProfile()
		os.Exit(1)
	}
}

const version = "0.1.0"

// parse parses flags interspersed with positional arguments.
func parse(fs *flag.FlagSet, args []string) []string {
	var pos []string
	for {
		fs.Parse(args)
		args = fs.Args()
		if len(args) == 0 {
			return pos
		}
		if args[0] == "--" {
			return append(pos, args[1:]...)
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func cutPrefix(s, p string) (string, bool) { return strings.CutPrefix(s, p) }

func mustSize(s string) int64 {
	s = strings.TrimSpace(strings.ToUpper(s))
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "K"):
		mult, s = 1<<10, s[:len(s)-1]
	case strings.HasSuffix(s, "M"):
		mult, s = 1<<20, s[:len(s)-1]
	case strings.HasSuffix(s, "G"):
		mult, s = 1<<30, s[:len(s)-1]
	case strings.HasSuffix(s, "T"):
		mult, s = 1<<40, s[:len(s)-1]
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gft: bad size %q\n", s)
		os.Exit(2)
	}
	return int64(v * float64(mult))
}

func bg() context.Context { return context.Background() }

func openWT() (*client.Worktree, error) {
	root, err := client.Find(".")
	if err != nil {
		return nil, err
	}
	return client.Open(root)
}

// apiClient returns a client for repository queries: the working copy's
// server if inside one, else --server / $GFT_SERVER.
func apiClient(server string) (*api.Client, *client.Worktree) {
	if server != "" {
		return api.NewClientToken(server, os.Getenv("GFT_TOKEN")), nil
	}
	if mc, _ := vfs.FindMount("."); mc != nil {
		var h vfs.MountHead
		if mc.Call("/head", nil, &h) == nil {
			return api.NewClient(mc.Meta.Server), &client.Worktree{Head: client.Head{Changeset: h.Changeset, Root: h.Root}}
		}
	}
	if wt, err := openWT(); err == nil {
		return wt.API, wt
	}
	return api.NewClientToken(envOr("GFT_SERVER", "http://127.0.0.1:7070"), os.Getenv("GFT_TOKEN")), nil
}

// defaultRev is HEAD's changeset in a working copy, else main.
func defaultRev(wt *client.Worktree) string {
	if wt != nil && !wt.Head.Changeset.IsZero() {
		return wt.Head.Changeset.String()
	}
	return "main"
}

func revOr(rev string, wt *client.Worktree) string {
	if rev == "" || rev == "HEAD" || rev == "." {
		return defaultRev(wt)
	}
	if strings.HasPrefix(rev, "HEAD~") && wt != nil {
		return defaultRev(wt) + rev[4:]
	}
	return rev
}

func cmdImport(args []string) error {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	data := fs.String("data", "", "server data directory (server must be stopped)")
	storeNodes := fs.String("store", "", "import into a storage fleet instead (comma-separated node URLs)")
	gitDir := fs.String("git", ".", "git repository")
	var refs multi
	fs.Var(&refs, "ref", "git ref to import (repeatable; default HEAD -> main)")
	parse(fs, args)
	if *storeNodes != "" {
		r, err := repo.Open(store.NewSharded(strings.Split(*storeNodes, ","), 1<<30))
		if err != nil {
			return err
		}
		st, err := gitimport.Import(bg(), r, gitimport.Options{GitDir: *gitDir, Refs: refs, RefMap: map[string]string{"HEAD": "main"}})
		if err != nil {
			return err
		}
		fmt.Printf("imported %d changesets, %d blobs into %d shards in %s\n", st.Changesets, st.Blobs, len(strings.Split(*storeNodes, ",")), st.Elapsed.Round(time.Millisecond))
		return nil
	}
	if *data == "" {
		return errors.New("--data or --store is required")
	}
	db, err := store.OpenDB(*data, 1<<30)
	if err != nil {
		return err
	}
	r, err := repo.Open(db)
	if err != nil {
		return err
	}
	refMap := map[string]string{"HEAD": "main"}
	st, err := gitimport.Import(bg(), r, gitimport.Options{GitDir: *gitDir, Refs: refs, RefMap: refMap, Progress: func(s gitimport.Stats) {
		fmt.Fprintf(os.Stderr, "\r%d changesets, %d blobs (%.1f MB), %.0f cs/s", s.Changesets, s.Blobs, float64(s.BlobBytes)/1e6, float64(s.Changesets)/s.Elapsed.Seconds())
	}})
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return err
	}
	fmt.Printf("imported %d changesets, %d blobs (%.1f MB) in %s\n", st.Changesets, st.Blobs, float64(st.BlobBytes)/1e6, st.Elapsed.Round(time.Millisecond))
	if os.Getenv("GFT_DEBUG") != "" {
		fmt.Println(db.Stats())
	}
	// Reopen once so pebble drops write-ahead log files left from the bulk load.
	if err := db.Close(); err != nil {
		return err
	}
	db2, err := store.OpenDB(*data, 64<<20)
	if err != nil {
		return err
	}
	time.Sleep(200 * time.Millisecond) // obsolete files are deleted asynchronously
	return db2.Close()
}

func cmdClone(args []string) error {
	fs := flag.NewFlagSet("clone", flag.ExitOnError)
	var sparse multi
	fs.Var(&sparse, "sparse", "directory to materialize (repeatable)")
	objects := fs.String("objects", "", "object endpoint: daemon socket (unix://...) or cache URL")
	rev := fs.String("rev", "main", "revision to check out")
	author := fs.String("author", "", "author identity (default from git config or $USER)")
	token := fs.String("token", "", "bearer token for an access-controlled server (or $GFT_TOKEN)")
	pos := parse(fs, args)
	if len(pos) != 2 {
		return errors.New("usage: gft clone <server> <dir>")
	}
	if *author == "" {
		*author = defaultAuthor()
	}
	start := time.Now()
	wt, err := client.Init(pos[1], client.Config{Server: pos[0], Objects: *objects, Sparse: sparse, Author: *author, Token: *token})
	if err != nil {
		return err
	}
	wt.Head.Branch = *rev
	st, err := wt.Checkout(bg(), *rev, false)
	if err != nil {
		return err
	}
	fmt.Printf("checked out %s: %d files, %.1f MB, %d trees in %s\n", wt.Head.Changeset.Short(), st.Files, float64(st.Bytes)/1e6, st.Trees, time.Since(start).Round(time.Millisecond))
	if len(st.Restricted) > 0 {
		fmt.Printf("skipped %d director(ies) you do not have access to\n", len(st.Restricted))
	}
	return nil
}

func defaultAuthor() string {
	name, _ := exec.Command("git", "config", "user.name").Output()
	email, _ := exec.Command("git", "config", "user.email").Output()
	n, e := strings.TrimSpace(string(name)), strings.TrimSpace(string(email))
	if n == "" {
		n = os.Getenv("USER")
	}
	if e == "" {
		return n
	}
	return n + " <" + e + ">"
}

func cmdCheckout(args []string) error {
	fs := flag.NewFlagSet("checkout", flag.ExitOnError)
	force := fs.Bool("force", false, "overwrite local modifications")
	pos := parse(fs, args)
	if len(pos) != 1 {
		return errors.New("usage: gft checkout <rev>")
	}
	if mc, _ := vfs.FindMount("."); mc != nil {
		start := time.Now()
		var ci api.ChangesetInfo
		if err := mc.Call("/checkout", map[string]string{"rev": pos[0], "branch": ""}, &ci); err != nil {
			return err
		}
		fmt.Printf("mount now shows %s (no files written) in %s\n", ci.ID.Short(), time.Since(start).Round(time.Millisecond))
		return nil
	}
	wt, err := openWT()
	if err != nil {
		return err
	}
	start := time.Now()
	ci, err := wt.API.Resolve(bg(), revOr(pos[0], wt))
	if err != nil {
		return err
	}
	if refs, err := wt.API.Refs(bg()); err == nil {
		if _, isRef := refs[pos[0]]; isRef {
			wt.Head.Branch = pos[0]
		}
	}
	st, err := wt.Checkout(bg(), ci.ID.String(), *force)
	if err != nil {
		return err
	}
	fmt.Printf("at %s: %d files written, %d deleted in %s\n", ci.ID.Short(), st.Files, st.Deleted, time.Since(start).Round(time.Millisecond))
	return nil
}

func looksLikeID(s string) bool {
	if len(s) < 8 {
		return false
	}
	for _, c := range s {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}

func cmdSparse(args []string) error {
	if len(args) == 0 {
		args = []string{"list"}
	}
	wt, err := openWT()
	if err != nil {
		return err
	}
	switch args[0] {
	case "list":
		if len(wt.Config.Sparse) == 0 {
			fmt.Println("(full checkout)")
		}
		for _, s := range wt.Config.Sparse {
			fmt.Println(s)
		}
		return nil
	case "add":
		wt.Config.Sparse = append(wt.Config.Sparse, args[1:]...)
	case "set":
		wt.Config.Sparse = args[1:]
	default:
		return fmt.Errorf("unknown sparse subcommand %q", args[0])
	}
	start := time.Now()
	st, err := wt.ApplySparse(bg())
	if err != nil {
		return err
	}
	fmt.Printf("sparse updated: %d files written, %d removed in %s\n", st.Files, st.Deleted, time.Since(start).Round(time.Millisecond))
	return nil
}

func cmdStatus(args []string) error {
	if mc, _ := vfs.FindMount("."); mc != nil {
		var h vfs.MountHead
		if err := mc.Call("/head", nil, &h); err != nil {
			return err
		}
		var st []vfs.FileState
		if err := mc.Call("/status", nil, &st); err != nil {
			return err
		}
		branch := h.Branch
		if branch == "" {
			branch = "(detached)"
		}
		fmt.Printf("at %s on %s (virtual mount)\n", h.Changeset.Short(), branch)
		for _, s := range st {
			fmt.Printf("%s %s\n", s.State, s.Path)
		}
		return nil
	}
	wt, err := openWT()
	if err != nil {
		return err
	}
	st, err := wt.Status(bg())
	if err != nil {
		return err
	}
	branch := wt.Head.Branch
	if branch == "" {
		branch = "(detached)"
	}
	fmt.Printf("at %s on %s\n", wt.Head.Changeset.Short(), branch)
	for _, s := range st {
		fmt.Printf("%c %s\n", s.State, s.Path)
	}
	return nil
}

func cmdCommit(args []string) error {
	fs := flag.NewFlagSet("commit", flag.ExitOnError)
	msg := fs.String("m", "", "message")
	land := fs.String("land", "", "also land onto this ref")
	paths := parse(fs, args)
	if *msg == "" {
		return errors.New("commit message required (-m)")
	}
	if mc, _ := vfs.FindMount("."); mc != nil {
		var out struct {
			ID    object.ID
			Paths int
		}
		start := time.Now()
		if err := mc.Call("/commit", map[string]string{"message": *msg}, &out); err != nil {
			return err
		}
		fmt.Printf("created %s (%d paths) in %s\n", out.ID.Short(), out.Paths, time.Since(start).Round(time.Millisecond))
		if *land != "" {
			return mountLand(mc, *land)
		}
		return nil
	}
	wt, err := openWT()
	if err != nil {
		return err
	}
	start := time.Now()
	id, st, err := wt.Commit(bg(), *msg, paths)
	if err != nil {
		return err
	}
	fmt.Printf("created %s (%d paths) in %s\n", id.Short(), len(st), time.Since(start).Round(time.Millisecond))
	if *land != "" {
		return doLand(wt, *land)
	}
	return nil
}

func doLand(wt *client.Worktree, ref string) error {
	start := time.Now()
	res, err := wt.Land(bg(), ref)
	if err != nil {
		return err
	}
	how := "fast-forward"
	if res.Rebased > 0 {
		how = fmt.Sprintf("rebased %d changeset(s)", res.Rebased)
	} else if res.Merged {
		how = "merged"
	}
	wt.Head.Branch = ref
	wt.SaveHead()
	fmt.Printf("landed %s on %s: %s -> %s (%s, %d attempt(s)) in %s\n", wt.Head.Changeset.Short(), ref, res.Old.Short(), res.New.Short(), how, res.Attempts, time.Since(start).Round(time.Millisecond))
	return nil
}

func mountLand(mc *vfs.MountClient, ref string) error {
	start := time.Now()
	var res repo.LandResult
	if err := mc.Call("/land", map[string]string{"ref": ref}, &res); err != nil {
		return err
	}
	if ref == "" {
		ref = "tracked branch"
	}
	fmt.Printf("landed on %s: %s -> %s (%d rebased, %d attempt(s)) in %s; mount now shows %s\n", ref, res.Old.Short(), res.New.Short(),
		res.Rebased, res.Attempts, time.Since(start).Round(time.Millisecond), res.New.Short())
	return nil
}

func cmdLand(args []string) error {
	fs := flag.NewFlagSet("land", flag.ExitOnError)
	ref := fs.String("ref", "", "ref to land onto (default: tracked branch or main)")
	parse(fs, args)
	if mc, _ := vfs.FindMount("."); mc != nil {
		return mountLand(mc, *ref)
	}
	wt, err := openWT()
	if err != nil {
		return err
	}
	r := *ref
	if r == "" {
		r = wt.Head.Branch
	}
	if r == "" || looksLikeID(r) {
		r = "main"
	}
	return doLand(wt, r)
}

func cmdRebase(args []string) error {
	pos := parse(flag.NewFlagSet("rebase", flag.ExitOnError), args)
	if len(pos) != 1 {
		return errors.New("usage: gft rebase <onto>")
	}
	wt, err := openWT()
	if err != nil {
		return err
	}
	id, err := wt.API.Rebase(bg(), wt.Head.Changeset.String(), pos[0])
	if err != nil {
		return err
	}
	if _, err := wt.Checkout(bg(), id.String(), false); err != nil {
		return err
	}
	fmt.Printf("rebased onto %s: now at %s\n", pos[0], id.Short())
	return nil
}

func printChangeset(ci *api.ChangesetInfo, oneline bool) {
	if oneline {
		fmt.Printf("%s %s\n", ci.ID.Short(), firstLine(ci.Message))
		return
	}
	fmt.Printf("changeset %s\n", ci.ID)
	if len(ci.Parents) > 1 {
		var ps []string
		for _, p := range ci.Parents {
			ps = append(ps, p.Short())
		}
		fmt.Printf("Merge: %s\n", strings.Join(ps, " "))
	}
	fmt.Printf("Author: %s\n", ci.Author)
	loc := time.FixedZone("", int(ci.TZ)*60)
	fmt.Printf("Date:   %s\n", time.Unix(ci.Time, 0).In(loc).Format("Mon Jan 2 15:04:05 2006 -0700"))
	if ci.Origin != "" {
		fmt.Printf("Origin: %s\n", ci.Origin)
	}
	fmt.Println()
	for _, l := range strings.Split(strings.TrimRight(ci.Message, "\n"), "\n") {
		fmt.Printf("    %s\n", l)
	}
	fmt.Println()
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func cmdLog(args []string) error {
	fs := flag.NewFlagSet("log", flag.ExitOnError)
	rev := fs.String("rev", "", "revision (default HEAD)")
	n := fs.Int("n", 20, "limit (0 = all)")
	fp := fs.Bool("first-parent", false, "follow only first parents")
	oneline := fs.Bool("oneline", false, "one line per changeset")
	follow := fs.Bool("follow", false, "follow file history across renames")
	server := fs.String("server", "", "server URL")
	pos := parse(fs, args)
	c, wt := apiClient(*server)
	path := ""
	if len(pos) > 0 {
		path = pathArg(pos[0], wt)
	}
	out, err := c.Log(bg(), revOr(*rev, wt), path, *n, *fp, *follow)
	if err != nil {
		return err
	}
	for _, ci := range out {
		printChangeset(ci, *oneline)
	}
	return nil
}

// pathArg converts a path relative to the cwd into a repo path.
func pathArg(p string, wt *client.Worktree) string {
	if wt == nil {
		return strings.Trim(p, "/")
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	rel, err := filepath.Rel(wt.Root, abs)
	if err != nil || strings.HasPrefix(rel, "..") {
		return strings.Trim(p, "/")
	}
	if rel == "." {
		return ""
	}
	return filepath.ToSlash(rel)
}

func cmdBlame(args []string) error {
	fs := flag.NewFlagSet("blame", flag.ExitOnError)
	rev := fs.String("rev", "", "revision")
	server := fs.String("server", "", "server URL")
	pos := parse(fs, args)
	if len(pos) != 1 {
		return errors.New("usage: gft blame <path>")
	}
	c, wt := apiClient(*server)
	out, err := c.Blame(bg(), revOr(*rev, wt), pathArg(pos[0], wt))
	if err != nil {
		return err
	}
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	for _, b := range out {
		who := b.Author
		if i := strings.IndexByte(who, '<'); i > 0 {
			who = strings.TrimSpace(who[:i])
		}
		if len(who) > 16 {
			who = who[:16]
		}
		fmt.Fprintf(w, "%s (%-16s %s %5d) %s\n", b.Changeset.Short(), who, time.Unix(b.Time, 0).Format("2006-01-02"), b.Line, b.Text)
	}
	return nil
}

func cmdShow(args []string) error {
	fs := flag.NewFlagSet("show", flag.ExitOnError)
	server := fs.String("server", "", "server URL")
	stat := fs.Bool("stat", false, "list files only")
	pos := parse(fs, args)
	c, wt := apiClient(*server)
	rev := ""
	if len(pos) > 0 {
		rev = pos[0]
	}
	ci, err := c.Changeset(bg(), revOr(rev, wt))
	if err != nil {
		return err
	}
	printChangeset(ci, false)
	if *stat {
		for _, ch := range ci.Changes {
			fmt.Printf(" %s %s\n", map[bool]string{true: "D", false: "M"}[ch.Mode == object.ModeNone], ch.Path)
		}
		return nil
	}
	return c.Patch(bg(), os.Stdout, "", ci.ID.String(), nil)
}

func cmdDiff(args []string) error {
	fs := flag.NewFlagSet("diff", flag.ExitOnError)
	from := fs.String("from", "", "base revision")
	to := fs.String("to", "", "target revision")
	stat := fs.Bool("stat", false, "list changed files only")
	server := fs.String("server", "", "server URL")
	paths := parse(fs, args)
	c, wt := apiClient(*server)
	for i := range paths {
		paths[i] = pathArg(paths[i], wt)
	}
	if *from == "" && *to == "" {
		if wt == nil {
			return errors.New("not in a working copy; use --from/--to")
		}
		return localDiff(wt, paths, *stat)
	}
	toRev := revOr(*to, wt)
	if *stat {
		d, err := c.Diff(bg(), *from, toRev, paths)
		if err != nil {
			return err
		}
		for _, ch := range d.Changes {
			state := "M"
			if ch.OldMode == object.ModeNone {
				state = "A"
			} else if ch.NewMode == object.ModeNone {
				state = "D"
			}
			fmt.Printf("%s %s\n", state, ch.Path)
		}
		return nil
	}
	return c.Patch(bg(), os.Stdout, *from, toRev, paths)
}

func localDiff(wt *client.Worktree, paths []string, stat bool) error {
	st, err := wt.Status(bg())
	if err != nil {
		return err
	}
	idx, err := wt.LoadIndex()
	if err != nil {
		return err
	}
	match := func(p string) bool {
		if len(paths) == 0 {
			return true
		}
		for _, q := range paths {
			if q == "" || p == q || strings.HasPrefix(p, q+"/") {
				return true
			}
		}
		return false
	}
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	for _, s := range st {
		if !match(s.Path) {
			continue
		}
		if stat {
			fmt.Fprintf(w, "%c %s\n", s.State, s.Path)
			continue
		}
		var old, cur []byte
		na, nb := "a/"+s.Path, "b/"+s.Path
		if e := idx[s.Path]; e != nil {
			_, old, err = wt.ReadObject(bg(), e.Blob)
			if err != nil {
				return err
			}
		} else {
			na = "/dev/null"
		}
		if s.State != 'D' {
			cur, err = os.ReadFile(filepath.Join(wt.Root, s.Path))
			if err != nil {
				return err
			}
		} else {
			nb = "/dev/null"
		}
		fmt.Fprintf(w, "diff --gft a/%s b/%s\n", s.Path, s.Path)
		diff.Unified(w, na, nb, old, cur, 3)
	}
	return nil
}

func cmdMergeBase(args []string) error {
	fs := flag.NewFlagSet("merge-base", flag.ExitOnError)
	server := fs.String("server", "", "server URL")
	pos := parse(fs, args)
	if len(pos) != 2 {
		return errors.New("usage: gft merge-base <a> <b>")
	}
	c, wt := apiClient(*server)
	bases, err := c.MergeBases(bg(), revOr(pos[0], wt), revOr(pos[1], wt))
	if err != nil {
		return err
	}
	for _, b := range bases {
		fmt.Println(b)
	}
	return nil
}

func cmdIsAncestor(args []string) error {
	fs := flag.NewFlagSet("is-ancestor", flag.ExitOnError)
	server := fs.String("server", "", "server URL")
	pos := parse(fs, args)
	if len(pos) != 2 {
		return errors.New("usage: gft is-ancestor <a> <b>")
	}
	c, wt := apiClient(*server)
	ok, err := c.IsAncestor(bg(), revOr(pos[0], wt), revOr(pos[1], wt))
	if err != nil {
		return err
	}
	fmt.Println(ok)
	if !ok {
		os.Exit(1)
	}
	return nil
}

func cmdLs(args []string) error {
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	rev := fs.String("rev", "", "revision")
	server := fs.String("server", "", "server URL")
	pos := parse(fs, args)
	c, wt := apiClient(*server)
	p := ""
	if len(pos) > 0 {
		p = pathArg(pos[0], wt)
	}
	ents, err := c.Ls(bg(), revOr(*rev, wt), p)
	if err != nil {
		return err
	}
	for _, e := range ents {
		suffix := ""
		if e.Mode == object.ModeDir {
			suffix = "/"
		}
		fmt.Printf("%-8s %s  %s%s\n", e.Mode, e.ID.Short(), e.Name, suffix)
	}
	return nil
}

func cmdCat(args []string) error {
	fs := flag.NewFlagSet("cat", flag.ExitOnError)
	rev := fs.String("rev", "", "revision")
	server := fs.String("server", "", "server URL")
	pos := parse(fs, args)
	if len(pos) != 1 {
		return errors.New("usage: gft cat <path>")
	}
	c, wt := apiClient(*server)
	return c.Cat(bg(), os.Stdout, revOr(*rev, wt), pathArg(pos[0], wt))
}

func cmdFind(args []string) error {
	fs := flag.NewFlagSet("find", flag.ExitOnError)
	rev := fs.String("rev", "", "revision")
	in := fs.String("in", "", "directory to search")
	n := fs.Int("n", 100, "limit")
	server := fs.String("server", "", "server URL")
	pos := parse(fs, args)
	if len(pos) != 1 {
		return errors.New("usage: gft find <substring>")
	}
	c, wt := apiClient(*server)
	out, err := c.Search(bg(), revOr(*rev, wt), *in, pos[0], *n)
	if err != nil {
		return err
	}
	for _, p := range out {
		fmt.Println(p)
	}
	return nil
}

func cmdGrep(args []string) error {
	fs := flag.NewFlagSet("grep", flag.ExitOnError)
	rev := fs.String("rev", "", "revision")
	in := fs.String("in", "", "directory to search")
	ic := fs.Bool("i", false, "ignore case")
	fixed := fs.Bool("F", false, "fixed string")
	n := fs.Int("n", 1000, "maximum matches")
	server := fs.String("server", "", "server URL")
	pos := parse(fs, args)
	if len(pos) != 1 {
		return errors.New("usage: gft grep <regexp>")
	}
	c, wt := apiClient(*server)
	res, err := c.Grep(bg(), revOr(*rev, wt), pos[0], *in, *fixed, *ic, *n)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	for _, m := range res.Matches {
		fmt.Fprintf(w, "%s:%d:%s\n", m.Path, m.Line, m.Text)
	}
	if res.Truncated {
		fmt.Fprintf(w, "(stopped after %d matches)\n", len(res.Matches))
	}
	return nil
}

func printReview(st *api.ReviewStatus) {
	state := "ready to land"
	if !st.Satisfied {
		state = "needs review"
	}
	fmt.Printf("%s: %s\n", st.Changeset.Short(), state)
	for _, r := range st.Rules {
		mark := "ok"
		if !r.Satisfied {
			mark = "MISSING"
		}
		fmt.Printf("  %-24s %d/%d approvals %-8s %s\n", r.Path+"/", len(r.Approvals), r.Required, mark, strings.Join(r.Approvals, ", "))
		for _, f := range r.Files {
			fmt.Printf("      needs review: %s\n", f)
		}
		for _, f := range r.Exempted {
			fmt.Printf("      exempt:       %s\n", f)
		}
	}
}

func cmdApprove(args []string) error {
	fs := flag.NewFlagSet("approve", flag.ExitOnError)
	server := fs.String("server", "", "server URL")
	pos := parse(fs, args)
	if len(pos) != 1 {
		return errors.New("usage: gft approve <rev>")
	}
	c, wt := apiClient(*server)
	st, err := c.Approve(bg(), revOr(pos[0], wt))
	if err != nil {
		return err
	}
	printReview(st)
	return nil
}

func cmdReview(args []string) error {
	fs := flag.NewFlagSet("review", flag.ExitOnError)
	server := fs.String("server", "", "server URL")
	ref := fs.String("ref", "main", "ref the change would land on")
	pos := parse(fs, args)
	c, wt := apiClient(*server)
	rev := ""
	if len(pos) > 0 {
		rev = pos[0]
	}
	st, err := c.ReviewStatus(bg(), revOr(rev, wt), *ref)
	if err != nil {
		return err
	}
	printReview(st)
	return nil
}

func cmdRefs(args []string) error {
	fs := flag.NewFlagSet("refs", flag.ExitOnError)
	server := fs.String("server", "", "server URL")
	parse(fs, args)
	c, _ := apiClient(*server)
	refs, err := c.Refs(bg())
	if err != nil {
		return err
	}
	for name, id := range refs {
		fmt.Printf("%s %s\n", id, name)
	}
	return nil
}

func cmdWrite(args []string) error {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	parent := fs.String("parent", "main", "parent revision")
	land := fs.String("land", "", "land onto this ref")
	msg := fs.String("m", "", "message")
	server := fs.String("server", "", "server URL")
	author := fs.String("author", "", "author")
	pos := parse(fs, args)
	if *msg == "" || len(pos) == 0 {
		return errors.New("usage: gft write -m <msg> [--parent R] [--land REF] path=@file | path=-delete ...")
	}
	c, _ := apiClient(*server)
	if *author == "" {
		*author = defaultAuthor()
	}
	req := &api.WriteRequest{Parent: *parent, Author: *author, Message: *msg, Land: *land}
	for _, a := range pos {
		p, v, ok := strings.Cut(a, "=")
		if !ok {
			return fmt.Errorf("bad file spec %q", a)
		}
		switch {
		case v == "-delete":
			req.Files = append(req.Files, api.WriteFile{Path: p, Delete: true})
		case strings.HasPrefix(v, "@"):
			b, err := os.ReadFile(v[1:])
			if err != nil {
				return err
			}
			req.Files = append(req.Files, api.WriteFile{Path: p, Content: b})
		default:
			req.Files = append(req.Files, api.WriteFile{Path: p, Content: []byte(v)})
		}
	}
	resp, err := c.Write(bg(), req)
	if err != nil {
		return err
	}
	fmt.Println(resp.ID)
	if resp.Land != nil {
		fmt.Printf("landed: %s -> %s\n", resp.Land.Old.Short(), resp.Land.New.Short())
	}
	return nil
}

func cmdInfo(args []string) error {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	server := fs.String("server", "", "server or tier URL")
	parse(fs, args)
	c, wt := apiClient(*server)
	info, err := c.Info(bg())
	if err != nil {
		return err
	}
	for k, v := range info {
		fmt.Printf("%s: %v\n", k, v)
	}
	if wt != nil {
		h, m, b, n := wt.CacheStats()
		fmt.Printf("local cache: %d objects, %.1f MB, %d hits, %d misses\n", n, float64(b)/1e6, h, m)
	}
	return nil
}
