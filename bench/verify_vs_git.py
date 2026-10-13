#!/usr/bin/env python3
"""Cross-check graft history answers against git for every file in a repo.

Usage: verify_vs_git.py <git-dir> <server-url> [max-files]
Compares, per file: path log (set of commits that touched it, via origin
ids) and blame (commit per line). Reports mismatches.
"""
import json, subprocess, sys, urllib.parse, urllib.request

git_dir, server = sys.argv[1], sys.argv[2]
max_files = int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3].isdigit() else 10**9

def api(endpoint, **q):
    with urllib.request.urlopen(f"{server}{endpoint}?{urllib.parse.urlencode(q)}") as r:
        return json.load(r)

def git(*a):
    return subprocess.run(["git", "-C", git_dir, *a], capture_output=True, text=True, errors="replace", check=True).stdout

files = git("ls-files").split("\n")
files = [f for f in files if f][:max_files]
origin = {}
def orig(cs):
    if cs not in origin:
        origin[cs] = api("/v1/changeset", id=cs)["origin"].removeprefix("git:")
    return origin[cs]

log_bad = blame_bad = 0
lines_total = lines_same = 0
for f in files:
    g = git("log", "--format=%H", "--full-history", "--", f).split()
    ours = [orig(c["id"]) for c in api("/v1/log", rev="main", path=f, limit=0)]
    # git's simplified history may omit TREESAME merges; compare as sets of non-merge commits
    gset = set(g); oset = set(ours)
    if not set(git("log", "--format=%H", "--no-merges", "--", f).split()) <= oset or not oset <= gset:
        log_bad += 1
        print("LOG MISMATCH", f, len(g), len(ours))
    gb = []
    try:
        out = git("blame", "--porcelain", "--", f)
    except subprocess.CalledProcessError:
        continue
    for line in out.split("\n"):
        parts = line.split(" ")
        if len(parts) >= 3 and len(parts[0]) == 40 and parts[1].isdigit() and parts[2].isdigit():
            gb.append(parts[0])
    try:
        ob = [orig(b["changeset"]) for b in api("/v1/blame", rev="main", path=f)]
    except Exception as e:
        print("BLAME ERROR", f, e); blame_bad += 1; continue
    lines_total += max(len(gb), len(ob))
    lines_same += sum(1 for a, b in zip(gb, ob) if a == b)
    if gb != ob and "-v" in sys.argv:
        diffs = sum(1 for a, b in zip(gb, ob) if a != b) + abs(len(gb) - len(ob))
        blame_bad += 1
        print(f"BLAME MISMATCH {f}: {diffs}/{len(gb)} lines differ")
print(f"files={len(files)} path-log mismatches={log_bad} blame line agreement={lines_same}/{lines_total} ({100*lines_same/max(1,lines_total):.2f}%)")
