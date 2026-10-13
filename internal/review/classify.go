// Package review classifies changes so approval rules can tell sensitive
// logic changes from trivial ones (comment-only, whitespace-only, docs),
// letting human reviewers focus on the consequential changes.
package review

import (
	"bytes"
	"path"
	"strings"
)

// Class describes what kind of change a file modification is.
type Class string

const (
	Logic      Class = "logic"      // anything that may change behaviour
	Comments   Class = "comments"   // only comments and/or whitespace changed
	Whitespace Class = "whitespace" // only insignificant whitespace changed
	Docs       Class = "docs"       // documentation files
)

type syntax struct {
	line        []string // line-comment introducers
	blockOpen   string
	blockClose  string
	quotes      string // string delimiters
	indentation bool   // leading whitespace is significant
}

var (
	cLike  = syntax{line: []string{"//"}, blockOpen: "/*", blockClose: "*/", quotes: "\"'`"}
	hash   = syntax{line: []string{"#"}, quotes: "\"'"}
	python = syntax{line: []string{"#"}, quotes: "\"'", indentation: true}
	yaml   = syntax{line: []string{"#"}, quotes: "\"'", indentation: true}
	sql    = syntax{line: []string{"--"}, blockOpen: "/*", blockClose: "*/", quotes: "'\""}
	lisp   = syntax{line: []string{";"}, quotes: "\""}
)

func syntaxFor(p string) (syntax, bool) {
	base := path.Base(p)
	switch base {
	case "Makefile", "Dockerfile", "BUILD", "WORKSPACE", "CMakeLists.txt":
		return hash, true
	}
	switch strings.ToLower(path.Ext(p)) {
	case ".go", ".c", ".h", ".cc", ".cpp", ".cxx", ".hpp", ".hh", ".java", ".js", ".jsx", ".mjs", ".ts", ".tsx",
		".rs", ".swift", ".kt", ".kts", ".scala", ".cs", ".proto", ".dart", ".m", ".mm", ".php", ".css", ".scss", ".groovy", ".zig":
		return cLike, true
	case ".sh", ".bash", ".zsh", ".rb", ".pl", ".r", ".toml", ".conf", ".cfg", ".ini", ".mk", ".bzl", ".tf", ".nix", ".cmake":
		return hash, true
	case ".py", ".pyi":
		return python, true
	case ".yaml", ".yml":
		return yaml, true
	case ".sql", ".lua", ".hs":
		return sql, true
	case ".el", ".lisp", ".clj", ".scm":
		return lisp, true
	}
	return syntax{}, false
}

// IsDoc reports whether a path is documentation.
func IsDoc(p string) bool {
	switch strings.ToLower(path.Ext(p)) {
	case ".md", ".markdown", ".rst", ".adoc", ".txt":
		return true
	}
	return strings.HasPrefix(path.Base(p), "README") || strings.HasPrefix(path.Base(p), "CHANGELOG")
}

// stripComments removes comments, keeping string literals intact.
func stripComments(src []byte, s syntax) []byte {
	var out bytes.Buffer
	for i := 0; i < len(src); {
		c := src[i]
		if strings.IndexByte(s.quotes, c) >= 0 {
			j := i + 1
			for j < len(src) && src[j] != c {
				if src[j] == '\\' && c != '`' {
					j++
				}
				if c != '`' && src[j] == '\n' {
					break // unterminated single-line string
				}
				j++
			}
			if j < len(src) {
				j++
			}
			out.Write(src[i:min(j, len(src))])
			i = j
			continue
		}
		if s.blockOpen != "" && bytes.HasPrefix(src[i:], []byte(s.blockOpen)) {
			end := bytes.Index(src[i+len(s.blockOpen):], []byte(s.blockClose))
			if end < 0 {
				break
			}
			// Keep line structure so indentation-sensitive comparison works.
			out.Write(bytes.Repeat([]byte{'\n'}, bytes.Count(src[i:i+len(s.blockOpen)+end], []byte{'\n'})))
			out.WriteByte(' ')
			i += len(s.blockOpen) + end + len(s.blockClose)
			continue
		}
		lineComment := false
		for _, lc := range s.line {
			if bytes.HasPrefix(src[i:], []byte(lc)) {
				lineComment = true
				break
			}
		}
		if lineComment {
			for i < len(src) && src[i] != '\n' {
				i++
			}
			continue
		}
		out.WriteByte(c)
		i++
	}
	return out.Bytes()
}

// normalize removes insignificant whitespace: blank lines and trailing
// whitespace always; other whitespace runs collapse unless indentation is
// significant (then leading whitespace is kept).
func normalize(src []byte, s syntax) string {
	var lines []string
	for _, l := range strings.Split(string(src), "\n") {
		l = strings.TrimRight(l, " \t\r")
		if strings.TrimSpace(l) == "" {
			continue
		}
		if s.indentation {
			indent := len(l) - len(strings.TrimLeft(l, " \t"))
			lines = append(lines, l[:indent]+strings.Join(strings.Fields(l), " "))
		} else {
			lines = append(lines, strings.Join(strings.Fields(l), " "))
		}
	}
	if s.indentation {
		return strings.Join(lines, "\n")
	}
	return strings.Join(lines, " ")
}

// Classify reports what kind of change turns old into new at path. A nil
// old or new means the file was added or deleted.
func Classify(p string, old, new []byte) Class {
	if IsDoc(p) {
		return Docs
	}
	if old == nil || new == nil {
		return Logic
	}
	s, known := syntaxFor(p)
	if !known {
		if bytes.Equal(bytes.Join(bytes.Fields(old), nil), bytes.Join(bytes.Fields(new), nil)) && !bytes.Equal(old, new) {
			return Whitespace
		}
		return Logic
	}
	if normalize(old, s) == normalize(new, s) {
		return Whitespace
	}
	if normalize(stripComments(old, s), s) == normalize(stripComments(new, s), s) {
		return Comments
	}
	return Logic
}

// Exempt reports whether a change of class c is exempt under the listed
// exemption classes ("comments" also covers whitespace).
func Exempt(c Class, exempt []string) bool {
	for _, e := range exempt {
		switch Class(e) {
		case c:
			return true
		case Comments:
			if c == Whitespace {
				return true
			}
		}
	}
	return false
}
