package acl

import "testing"

func TestPolicy(t *testing.T) {
	p := (&Policy{
		Tokens:  map[string]string{"sha256:5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8": "alice", "b": "bob"},
		Groups:  map[string][]string{"sec": {"alice"}},
		Admins:  []string{"root"},
		Default: RoleRead,
		Roles:   map[string]Role{"bob": RoleWrite},
		Rules: []Rule{
			{Path: "secrets", Read: []string{"group:sec"}},
			{Path: "secrets/public", Read: []string{"*"}},
			{Path: "ro", Write: []string{}},
		},
	}).normalize()
	alice := p.Authenticate("password") // stored as a sha256 hash
	bob := p.Authenticate("b")
	if alice.Name != "alice" || bob.Name != "bob" || p.Authenticate("nope") != Anonymous {
		t.Fatal("authentication")
	}
	cases := []struct {
		pr          *Principal
		path        string
		read, write bool
	}{
		{alice, "README", true, false}, // default read
		{bob, "README", true, true},    // role override
		{alice, "secrets/k", true, false},
		{bob, "secrets/k", false, false},
		{bob, "secrets/public/x", true, true}, // more specific rule wins
		{bob, "secretsX/y", true, true},       // prefix match is per path segment
		{bob, "ro/file", true, false},         // empty write list: nobody but admins
		{Anonymous, "README", false, false},
	}
	for _, c := range cases {
		if got := p.CanRead(c.pr, c.path); got != c.read {
			t.Errorf("%s read %s = %v", c.pr.Name, c.path, got)
		}
		if got := p.CanWrite(c.pr, c.path); got != c.write {
			t.Errorf("%s write %s = %v", c.pr.Name, c.path, got)
		}
	}
}
