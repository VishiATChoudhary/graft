#!/usr/bin/env bash
# Storage comparison: graft vs git on the same history, with SHA-1 and
# SHA-256 git repositories, after git's default repack and after
# gc --aggressive (both with a commit-graph with changed-path filters).
# Usage: bench/storage.sh <git-repo-url-or-path> <name>
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
gft="$here/../bin/gft"
src="$1"; name="$2"
w="$here/work/storage-$name"; rm -rf "$w"; mkdir -p "$w"; cd "$w"
git clone -q --bare --single-branch --no-tags "$src" head.git
kb() { du -sk "$1" | cut -f1; }
pack() { # $1 dir, $2 aggressive?
  if [ "$2" = 1 ]; then git -C "$1" gc -q --aggressive --prune=now; else git -C "$1" repack -q -a -d -f; fi
  git -C "$1" commit-graph write --reachable --changed-paths 2>/dev/null
}
cp -R head.git sha1-default.git; pack sha1-default.git 0
cp -R head.git sha1-aggressive.git; pack sha1-aggressive.git 1
git init -q --bare --object-format=sha256 sha256.git
git -C head.git fast-export --reencode=yes --signed-tags=strip HEAD | git -C sha256.git fast-import --quiet
cp -R sha256.git sha256-default.git; pack sha256-default.git 0
cp -R sha256.git sha256-aggressive.git; pack sha256-aggressive.git 1
"$gft" import --data graft --git head.git >/dev/null
printf "%-20s %10s %10s %10s %10s %10s\n" repo graft git-sha1 git-sha1-aggr git-sha256 git-sha256-aggr
printf "%-20s %10s %10s %10s %10s %10s\n" "$name" "$(kb graft)" "$(kb sha1-default.git)" "$(kb sha1-aggressive.git)" "$(kb sha256-default.git)" "$(kb sha256-aggressive.git)"
