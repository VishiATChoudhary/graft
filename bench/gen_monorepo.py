#!/usr/bin/env python3
"""Generate a synthetic agent-scale monorepo as a bare git repository.

The shape mimics a trunk-based monorepo written mostly by agents: many
services/modules/packages, and a long linear history of small changes that
each touch a handful of files. Output is streamed into `git fast-import`.

Usage: gen_monorepo.py <out.git> [--files N] [--commits N] [--seed S]
"""
import argparse
import random
import subprocess
import sys

WORDS = ("agent model token cache shard merge rebase blame graph index tree blob "
         "server client daemon fetch commit land ref skip list derive unode path "
         "query budget quota retry stream frame chunk delta").split()


def gen_line(rng):
    k = rng.randint(3, 10)
    return "\t" * rng.randint(0, 2) + " ".join(rng.choice(WORDS) for _ in range(k)) + "\n"


def gen_file(rng, lines):
    return "".join(gen_line(rng) for _ in range(lines))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--files", type=int, default=100_000)
    ap.add_argument("--commits", type=int, default=20_000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--lines", type=int, default=60, help="mean lines per file")
    a = ap.parse_args()
    rng = random.Random(a.seed)

    subprocess.run(["git", "init", "-q", "--bare", "-b", "main", a.out], check=True)
    fi = subprocess.Popen(["git", "-C", a.out, "fast-import", "--quiet", "--done"],
                          stdin=subprocess.PIPE, bufsize=1 << 20)
    w = fi.stdin

    def data(b):
        w.write(b"data %d\n" % len(b))
        w.write(b)
        w.write(b"\n")

    # Layout: svcNN/modNN/pkgNN/fileNN.txt, about 50 files per package.
    per_pkg = 50
    pkgs = max(1, a.files // per_pkg)
    paths = []
    for p in range(pkgs):
        svc, rest = divmod(p, 100)
        mod, pkg = divmod(rest, 10)
        for f in range(per_pkg):
            paths.append(f"svc{svc:02d}/mod{mod}/pkg{pkg}/file{f:02d}.txt")
    contents = {}
    mark = 0
    t0 = 1_700_000_000

    # Initial import commit.
    file_marks = {}
    for p in paths:
        mark += 1
        c = gen_file(rng, max(5, int(rng.gauss(a.lines, a.lines / 3))))
        contents[p] = c.splitlines(keepends=True)
        w.write(b"blob\nmark :%d\n" % mark)
        data(c.encode())
        file_marks[p] = mark
    mark += 1
    w.write(b"commit refs/heads/main\nmark :%d\n" % mark)
    w.write(b"author Seed <seed@example.com> %d +0000\ncommitter Seed <seed@example.com> %d +0000\n" % (t0, t0))
    data(b"initial import")
    for p in paths:
        w.write(b"M 100644 :%d %s\n" % (file_marks[p], p.encode()))
    w.write(b"\n")

    # A set of hot files that agents touch repeatedly (long blame history).
    hot = rng.sample(paths, max(1, len(paths) // 1000))
    for i in range(a.commits):
        n = rng.choice((1, 1, 2, 2, 3, 5))
        touched = set()
        for _ in range(n):
            touched.add(rng.choice(hot) if rng.random() < 0.2 else rng.choice(paths))
        ops = []
        for p in sorted(touched):
            ls = contents[p]
            for _ in range(rng.randint(1, 4)):
                r = rng.random()
                pos = rng.randint(0, len(ls))
                if r < 0.5 or len(ls) < 3:
                    ls.insert(pos, gen_line(rng))
                elif r < 0.8:
                    ls[min(pos, len(ls) - 1)] = gen_line(rng)
                else:
                    del ls[min(pos, len(ls) - 1)]
            mark += 1
            w.write(b"blob\nmark :%d\n" % mark)
            data("".join(ls).encode())
            ops.append(b"M 100644 :%d %s\n" % (mark, p.encode()))
        mark += 1
        ts = t0 + 60 * (i + 1)
        agent = f"agent-{rng.randint(0, 999):03d}"
        w.write(b"commit refs/heads/main\nmark :%d\n" % mark)
        w.write(f"author {agent} <{agent}@agents.example> {ts} +0000\ncommitter {agent} <{agent}@agents.example> {ts} +0000\n".encode())
        data(f"{agent}: change {i}".encode())
        w.writelines(ops)
        w.write(b"\n")
        if (i + 1) % 5000 == 0:
            print(f"  {i + 1} commits", file=sys.stderr)
    w.write(b"done\n")
    w.close()
    if fi.wait() != 0:
        sys.exit("fast-import failed")
    subprocess.run(["git", "-C", a.out, "gc", "--quiet", "--aggressive"], check=True)
    subprocess.run(["git", "-C", a.out, "commit-graph", "write", "--reachable"], check=True)
    with open(f"{a.out}/hot_files.txt", "w") as f:
        f.write("\n".join(hot) + "\n")
    print(f"generated {len(paths)} files, {a.commits + 1} commits, {len(hot)} hot files -> {a.out}")


if __name__ == "__main__":
    main()
