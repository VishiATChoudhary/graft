# Access control and review gates

`gft serve --tier server --acl policy.json` enables a policy. Without one,
everything is allowed.

```json
{
  "tokens":    {"sha256:5e8848...": "alice", "ci-token-plain": "ci-bot"},
  "groups":    {"security": ["alice"], "payments-owners": ["alice", "carol"]},
  "anonymous": "none",
  "default":   "write",
  "roles":     {"group:contractors": "read"},
  "admins":    ["alice"],
  "rules": [
    {"path": "secrets",           "read":  ["group:security"], "write": ["group:security"]},
    {"path": "services/payments", "write": ["group:payments-owners"]}
  ],
  "reviews": [
    {"path": "services/payments", "approvals": 1, "approvers": ["group:payments-owners"],
     "exempt": ["comments", "docs"]}
  ]
}
```

## Principals and roles

* `tokens` map bearer tokens to principals. Store them as `sha256:<hex>` to
  keep plain tokens out of the file. Clients send them with `--token` on
  clone (saved in the working copy) or `GFT_TOKEN`.
* `groups` name sets of principals (`"*"` = every authenticated principal).
* Repository roles: `anonymous` (unauthenticated callers), `default` (every
  authenticated principal), `roles` (per principal or `group:x`, highest
  wins), `admins`. Roles are `none`, `read`, `write`, `admin`.

## Directory rules

Each rule restricts `read` and/or `write` below `path` to the listed
principals (`name`, `group:x`, `*`). For each permission the most specific
rule that mentions it applies; rules that don't mention a permission inherit
it. Admins bypass rules. An empty list (`"write": []`) means nobody but
admins.

Enforcement:

| operation | check |
|---|---|
| every API call | repository read |
| `cat`, `log <path>`, `blame`, `ls` | read on the path |
| changeset file lists, `diff`, `find`, `grep` | results filtered to readable paths |
| create changeset, `write` | write on every changed path |
| `land`, `refs/update` | write on every path that differs between the old and new tip (merges included) |
| deleting a ref | admin |
| object fetch | trees of read-restricted directories need read on that directory |

Object fetches are content-addressed, so the server indexes the tree of
every read-restricted directory whenever a changeset is created (derived
data, shared by all API servers; existing history is indexed at startup).
Blobs can only be named through those trees.

Restricted trees are flagged *private* in the fetch protocol:
* Daemons and regional caches pass them through without caching them, and
  always forward the caller's credentials upstream.
* An in-flight fetch is never shared between callers presenting different
  credentials.

Clients skip directories they cannot read and report how many they skipped.

## Review gates

A review rule requires `approvals` approvals from `approvers` before changes
under `path` can land. Each change is classified per file:

| class | meaning |
|---|---|
| `docs` | documentation files (`.md`, `.rst`, `.txt`, `README*`, ...) |
| `whitespace` | only insignificant whitespace changed |
| `comments` | only comments (and whitespace) changed |
| `logic` | anything else |

Comments are stripped per language, and string literals are respected (so
`"// not a comment"` stays code). Indentation is significant for Python and
YAML. Listing `comments` in `exempt` also exempts whitespace-only changes.

Approvals belong to a specific changeset (`gft approve <rev>`), so a new push
needs a fresh approval, and authors cannot approve their own changes.
`gft review` shows what a change still needs.
`land`, `write --land` and `refs/update` enforce the rules.
