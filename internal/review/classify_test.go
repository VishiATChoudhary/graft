package review

import "testing"

func TestClassify(t *testing.T) {
	cases := []struct {
		path     string
		old, new string
		want     Class
	}{
		{"a.go", "x := 1 // count\n", "x := 1 // number of items\n", Comments},
		{"a.go", "x := 1\n", "x := 1 /* note */\n", Comments},
		{"a.go", "x := 1\n", "x := 2\n", Logic},
		{"a.go", `s := "// not a comment"` + "\n", `s := "// changed"` + "\n", Logic},
		{"a.go", "func f() {\n\treturn\n}\n", "func f() {\n\n    return\n}\n", Whitespace},
		{"a.go", "/* a\nb */ x()\n", "/* a\nb\nc */ x()\n", Comments},
		{"a.py", "if x:\n    y()\n", "if x:\n    y()  # call y\n", Comments},
		{"a.py", "if x:\n    y()\nz()\n", "if x:\n    y()\n    z()\n", Logic}, // indentation matters
		{"a.sh", "echo hi # greet\n", "echo hi\n", Comments},
		{"README.md", "a", "b", Docs},
		{"a.go", "", "x", Logic},
		{"data.bin", "a b", "a  b", Whitespace},
	}
	for _, c := range cases {
		var old, new []byte = []byte(c.old), []byte(c.new)
		if c.old == "" {
			old = nil
		}
		if got := Classify(c.path, old, new); got != c.want {
			t.Errorf("%s %q -> %q: got %s want %s", c.path, c.old, c.new, got, c.want)
		}
	}
	if !Exempt(Whitespace, []string{"comments"}) || Exempt(Logic, []string{"comments", "docs"}) {
		t.Fatal("exemptions")
	}
}
