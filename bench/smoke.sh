#!/usr/bin/env bash
# End-to-end smoke test: import a real git repository, serve it through all
# three tiers, clone, commit, land, and cross-check history against git.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
gft="$here/../bin/gft"
work="$(mktemp -d)"
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$work"' EXIT
cd "$work"

git clone -q https://github.com/spf13/cobra.git src
"$gft" import --data srv --git src
"$gft" serve --tier server --data srv --listen 127.0.0.1:7570 & sleep 1
"$gft" serve --tier cache --upstream http://127.0.0.1:7570 --listen 127.0.0.1:7571 & sleep 0.5
"$gft" serve --tier daemon --upstream http://127.0.0.1:7571 --listen "unix://$work/d.sock" --dir "$work/dc" & sleep 0.5

"$gft" clone http://127.0.0.1:7570 wc --objects "unix://$work/d.sock" --author "CI <ci@example.com>"
diff -r -x .git -x .gft src wc
echo "working copy identical to git checkout"

python3 "$here/verify_vs_git.py" src http://127.0.0.1:7570 | tee verify.txt
grep -q "path-log mismatches=0" verify.txt

cd wc
echo "// smoke" >> command.go
"$gft" commit -m "smoke change" --land main
"$gft" log -n 2 --oneline
