#!/usr/bin/env python3
"""Benchmark graft (gft) against git on the workloads the Scion design targets.

Both systems serve from the same machine: git through `upload-pack` over
file:// (protocol v2, filters enabled), gft through its server, regional
cache and node daemon tiers on loopback. Every timing is wall-clock; CPU is
user+sys of all processes involved (client and server).

Usage: run.py --git <bare.git> --gft <data dir> [--reps 5] [--quick]
Writes bench/out/results.json and bench/out/RESULTS.md.
"""
import argparse
import json
import os
import random
import re
import resource
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
GFT = os.path.join(ROOT, "..", "bin", "gft")


def cpu_children():
    r = resource.getrusage(resource.RUSAGE_CHILDREN)
    return r.ru_utime + r.ru_stime


def run(cmd, cwd=None, check=True, env=None):
    p = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    if check and p.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed: {p.stderr.decode()[-2000:]}")
    return p


def info(url):
    if url.startswith("unix://"):
        out = run([GFT, "info", "--server", url]).stdout.decode()
        d = {}
        for line in out.splitlines():
            k, _, v = line.partition(": ")
            d[k] = v
        return {"cpu_seconds": float(d.get("cpu_seconds", 0))}
    with urllib.request.urlopen(url + "/v1/info") as r:
        return json.load(r)


class Tiers:
    """Starts gft server, cache and daemon tiers."""

    def __init__(self, data, work):
        self.work = work
        self.server = "http://127.0.0.1:7370"
        self.cache = "http://127.0.0.1:7371"
        self.sock = os.path.join(work, "gftd.sock")
        self.daemon = "unix://" + self.sock
        self.procs = []
        self._start(["serve", "--tier", "server", "--data", data, "--listen", "127.0.0.1:7370", "--mem", "2G"])
        self._wait(self.server)
        self._start(["serve", "--tier", "cache", "--upstream", self.server, "--listen", "127.0.0.1:7371", "--mem", "4G"])
        self._wait(self.cache)
        self.restart_daemon()

    def _start(self, args):
        p = subprocess.Popen([GFT] + args, stdout=subprocess.DEVNULL, stderr=open(os.path.join(self.work, "tiers.log"), "a"))
        self.procs.append(p)
        return p

    def _wait(self, url):
        for _ in range(100):
            try:
                info(url)
                return
            except Exception:
                time.sleep(0.05)
        raise RuntimeError("tier did not start: " + url)

    def restart_daemon(self):
        if getattr(self, "daemon_proc", None):
            self.daemon_proc.terminate()
            self.daemon_proc.wait()
            self.procs.remove(self.daemon_proc)
        d = os.path.join(self.work, "daemon-cache")
        shutil.rmtree(d, ignore_errors=True)
        self.daemon_proc = self._start(["serve", "--tier", "daemon", "--upstream", self.cache, "--listen", self.daemon, "--dir", d])
        for _ in range(100):
            if os.path.exists(self.sock):
                break
            time.sleep(0.05)
        time.sleep(0.1)

    def server_cpu(self):
        return sum(info(u)["cpu_seconds"] for u in (self.server, self.cache)) + info(self.daemon)["cpu_seconds"]

    def stop(self):
        for p in self.procs:
            p.send_signal(signal.SIGTERM)
        for p in self.procs:
            p.wait()


class Bench:
    def __init__(self, args):
        self.args = args
        self.git = os.path.abspath(args.git)
        self.giturl = "file://" + self.git
        self.work = tempfile.mkdtemp(prefix="gftbench-")
        self.results = []
        self.tiers = Tiers(os.path.abspath(args.gft), self.work)
        hot = open(os.path.join(self.git, "hot_files.txt")).read().split()
        self.hot = hot[0]
        self.reps = args.reps

    def path(self, *p):
        return os.path.join(self.work, *p)

    def measure(self, label, system, fn, setup=None, reps=None, server=False):
        """Run fn reps times; record median wall time and CPU."""
        walls, cpus = [], []
        for i in range(reps or self.reps):
            if setup:
                setup(i)
            c0 = cpu_children()
            s0 = self.tiers.server_cpu() if server else 0
            t0 = time.perf_counter()
            fn(i)
            wall = time.perf_counter() - t0
            cpu = cpu_children() - c0 + ((self.tiers.server_cpu() - s0) if server else 0)
            walls.append(wall)
            cpus.append(cpu)
        r = {"scenario": label, "system": system, "wall_s": statistics.median(walls), "cpu_s": statistics.median(cpus),
             "reps": len(walls)}
        self.results.append(r)
        print(f"  {system:<34} {r['wall_s']*1000:10.1f} ms   cpu {r['cpu_s']*1000:9.1f} ms", flush=True)
        return r

    def fresh(self, name):
        d = self.path(name)
        shutil.rmtree(d, ignore_errors=True)
        return d

    # ---------------------------------------------------------------- scenarios

    def checkout_sparse(self):
        print("\n[checkout-sparse] materialize one package (50 files) of 100k", flush=True)
        pkg = "svc07/mod3/pkg4"
        d = lambda i: self.path(f"sp{i}")
        clean = lambda i: shutil.rmtree(d(i), ignore_errors=True)
        self.measure("checkout-sparse", "git clone --filter=blob:none --sparse", lambda i: (
            run(["git", "clone", "-q", "--filter=blob:none", "--sparse", self.giturl, d(i)]),
            run(["git", "-C", d(i), "sparse-checkout", "set", pkg])), setup=clean)
        self.measure("checkout-sparse", "git ... + --depth 1 (best case)", lambda i: (
            run(["git", "clone", "-q", "--depth", "1", "--filter=blob:none", "--sparse", self.giturl, d(i)]),
            run(["git", "-C", d(i), "sparse-checkout", "set", pkg])), setup=clean)
        self.measure("checkout-sparse", "gft clone --sparse (cold, no daemon)", lambda i: run(
            [GFT, "clone", self.tiers.server, d(i), "--sparse", pkg]), setup=clean, server=True)
        run([GFT, "clone", self.tiers.server, self.fresh("warmup"), "--objects", self.tiers.daemon, "--sparse", pkg])
        self.measure("checkout-sparse", "gft clone --sparse (warm node daemon)", lambda i: run(
            [GFT, "clone", self.tiers.server, d(i), "--sparse", pkg, "--objects", self.tiers.daemon]), setup=clean, server=True)
        for i in range(self.reps):
            clean(i)

    def checkout_full(self):
        print("\n[checkout-full] full working copy (100k files)", flush=True)
        d = lambda i: self.path(f"full{i}")
        clean = lambda i: shutil.rmtree(d(i), ignore_errors=True)
        reps = max(1, self.reps // 2 + 1)
        self.measure("checkout-full", "git clone", lambda i: run(["git", "clone", "-q", self.giturl, d(i)]), setup=clean, reps=reps)
        self.measure("checkout-full", "git clone --depth 1 (best case)", lambda i: run(
            ["git", "clone", "-q", "--depth", "1", self.giturl, d(i)]), setup=clean, reps=reps)
        self.measure("checkout-full", "gft clone (cold, no daemon)", lambda i: run(
            [GFT, "clone", self.tiers.server, d(i)]), setup=clean, reps=reps, server=True)
        self.tiers.restart_daemon()
        self.measure("checkout-full", "gft clone (cold daemon, warm regional cache)", lambda i: run(
            [GFT, "clone", self.tiers.server, d(i), "--objects", self.tiers.daemon]),
            setup=lambda i: (clean(i), self.tiers.restart_daemon()), reps=reps, server=True)
        self.measure("checkout-full", "gft clone (warm node daemon)", lambda i: run(
            [GFT, "clone", self.tiers.server, d(i), "--objects", self.tiers.daemon]), setup=clean, reps=reps, server=True)
        for i in range(reps):
            clean(i)

    def mount_read(self):
        print("\n[mount-read] virtual checkout, then read one package (50 files)", flush=True)
        pkg = "svc07/mod3/pkg4"
        mnt = self.path("mnt")

        def go(i):
            state = self.path(f"mstate{i}")
            shutil.rmtree(state, ignore_errors=True)
            os.makedirs(mnt, exist_ok=True)
            p = subprocess.Popen([GFT, "mount", self.tiers.server, mnt, "--objects", self.tiers.daemon, "--state", state],
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in p.stdout:
                if b"mounted at" in line:
                    break
            for root, _, files in os.walk(os.path.join(mnt, pkg)):
                for f in files:
                    with open(os.path.join(root, f), "rb") as fh:
                        fh.read()
            p.send_signal(signal.SIGINT)
            p.wait()
        self.measure("mount-read", "gft mount + read package (warm daemon)", go, server=True)

    def setup_clones(self):
        """Full clones used by history/update benchmarks."""
        self.gclone = self.fresh("gclone")
        run(["git", "clone", "-q", self.giturl, self.gclone])
        run(["git", "-C", self.gclone, "commit-graph", "write", "--reachable"])
        self.fclone = self.fresh("fclone")
        run([GFT, "clone", self.tiers.server, self.fclone, "--objects", self.tiers.daemon])

    def update(self):
        print("\n[update] move a full working copy forward 100 changesets", flush=True)
        g, f = self.gclone, self.fclone
        self.measure("update-100", "git checkout", lambda i: run(["git", "-C", g, "checkout", "-q", "--detach", "main"]),
                     setup=lambda i: run(["git", "-C", g, "checkout", "-q", "--detach", "main~100"]))
        self.measure("update-100", "gft checkout", lambda i: run([GFT, "checkout", "main"], cwd=f),
                     setup=lambda i: run([GFT, "checkout", "main~100"], cwd=f), server=True)

    def status(self):
        print("\n[status] clean 100k-file working copy", flush=True)
        run(["git", "-C", self.gclone, "status", "--porcelain"])
        run([GFT, "status"], cwd=self.fclone)
        self.measure("status", "git status", lambda i: run(["git", "-C", self.gclone, "status", "--porcelain"]))
        self.measure("status", "gft status", lambda i: run([GFT, "status"], cwd=self.fclone))
        # With filesystem watchers: git's builtin fsmonitor daemon vs gft watch.
        run(["git", "-C", self.gclone, "config", "core.fsmonitor", "true"])
        run(["git", "-C", self.gclone, "config", "core.untrackedcache", "true"])
        for _ in range(3):
            run(["git", "-C", self.gclone, "status", "--porcelain"])
        run([GFT, "watch", "--background"], cwd=self.fclone)
        for _ in range(2):
            run([GFT, "status"], cwd=self.fclone)
        self.measure("status-watched", "git status (core.fsmonitor daemon)", lambda i: run(["git", "-C", self.gclone, "status", "--porcelain"]))
        self.measure("status-watched", "gft status (gft watch)", lambda i: run([GFT, "status"], cwd=self.fclone))
        run([GFT, "watch", "--stop"], cwd=self.fclone, check=False)
        run(["git", "-C", self.gclone, "fsmonitor--daemon", "stop"], check=False)

    def history(self):
        g = self.gclone
        s = ["--server", self.tiers.server]
        print(f"\n[blame] {self.hot} (hot file, long history)", flush=True)
        self.measure("blame", "git blame (local full clone)", lambda i: run(["git", "-C", g, "blame", "--", self.hot]))
        self.measure("blame", "gft blame (first query, derives)", lambda i: run([GFT, "blame", self.hot, "--rev", "main"] + s), reps=1, server=True)
        self.measure("blame", "gft blame (memoized)", lambda i: run([GFT, "blame", self.hot, "--rev", "main"] + s), server=True)

        print("\n[log-path] commits that touched a file", flush=True)
        self.measure("log-file", "git log -- <file>", lambda i: run(["git", "-C", g, "log", "--format=%H", "--", self.hot]))
        self.measure("log-file", "gft log <file>", lambda i: run([GFT, "log", "-n", "0", "--oneline", self.hot, "--rev", "main"] + s), server=True)
        print("\n[log-dir] last 50 commits under one service dir", flush=True)
        self.measure("log-dir", "git log -n 50 -- svc03/", lambda i: run(["git", "-C", g, "log", "-n", "50", "--format=%H", "--", "svc03/"]))
        self.measure("log-dir", "gft log -n 50 svc03", lambda i: run([GFT, "log", "-n", "50", "--oneline", "svc03", "--rev", "main"] + s), server=True)

        print("\n[ancestry] is main~19000 an ancestor of main?", flush=True)
        self.measure("is-ancestor", "git merge-base --is-ancestor", lambda i: run(["git", "-C", g, "merge-base", "--is-ancestor", "main~19000", "main"]))
        self.measure("is-ancestor", "gft is-ancestor", lambda i: run([GFT, "is-ancestor", "main~19000", "main"] + s), server=True)

        print("\n[grep] fixed-string search across all 100k files", flush=True)
        self.measure("grep", "git grep -F (in full clone)", lambda i: run(["git", "-C", g, "grep", "-F", "-c", "rebase blame graph"], check=False))
        run([GFT, "grep", "-F", "rebase blame graph"] + s)  # warm the server's caches
        self.measure("grep", "gft grep -F (server-side, no clone)", lambda i: run([GFT, "grep", "-F", "rebase blame graph"] + s), server=True)

        print("\n[ci-diff] files changed in svc03/ between main~5000 and main", flush=True)
        self.measure("ci-diff", "git diff --name-only (in full clone)", lambda i: run(
            ["git", "-C", g, "diff", "--name-only", "main~5000", "main", "--", "svc03/"]))
        self.measure("ci-diff", "gft diff --stat (API, no checkout)", lambda i: run(
            [GFT, "diff", "--stat", "--from", "main~5000", "--to", "main", "svc03"] + s), server=True)

    def concurrent_land(self, agents=16, per_agent=8):
        print(f"\n[land] {agents} agents x {per_agent} changes landed concurrently on main", flush=True)
        # git: shared bare repo copy so the benchmark repo is untouched.
        bare = self.fresh("land.git")
        run(["git", "clone", "-q", "--bare", self.git, bare])
        clones = []
        for a in range(agents):
            c = self.fresh(f"gagent{a}")
            run(["git", "clone", "-q", "--depth", "1", "--no-checkout", "file://" + bare, c])
            run(["git", "-C", c, "sparse-checkout", "set", f"svc{a:02d}/mod0/pkg0"])
            run(["git", "-C", c, "checkout", "-q", "main"])
            run(["git", "-C", c, "config", "user.name", f"agent{a}"])
            run(["git", "-C", c, "config", "user.email", f"a{a}@x"])
            clones.append(c)
        retries = [0] * agents

        def git_agent(a):
            c = clones[a]
            for k in range(per_agent):
                p = os.path.join(c, f"svc{a:02d}/mod0/pkg0/file00.txt")
                with open(p, "a") as fh:
                    fh.write(f"agent {a} change {k}\n")
                run(["git", "-C", c, "commit", "-qam", f"agent {a} change {k}"])
                while run(["git", "-C", c, "push", "-q", "origin", "HEAD:main"], check=False).returncode != 0:
                    retries[a] += 1
                    run(["git", "-C", c, "pull", "-q", "--rebase", "origin", "main"])

        def go(fn):
            ts = [threading.Thread(target=fn, args=(a,)) for a in range(agents)]
            for t in ts:
                t.start()
            for t in ts:
                t.join()

        r = self.measure("land", f"git commit + push (retry: pull --rebase)", lambda i: go(git_agent), reps=1)
        for a in range(agents):
            body = run(["git", "-C", bare, "show", f"main:svc{a:02d}/mod0/pkg0/file00.txt"]).stdout.decode()
            assert all(f"agent {a} change {k}\n" in body for k in range(per_agent)), f"git lost agent {a} changes"

        r["lands"] = agents * per_agent
        r["retries"] = sum(retries)
        r["lands_per_s"] = r["lands"] / r["wall_s"]
        print(f"    -> {r['lands_per_s']:.1f} lands/s, {r['retries']} push retries")

        # gft: agents land through the server; rebase happens server-side.
        wcs = []
        for a in range(agents):
            c = self.fresh(f"fagent{a}")
            run([GFT, "clone", self.tiers.server, c, "--objects", self.tiers.daemon, "--sparse", f"svc{a:02d}/mod0/pkg0", "--rev", "main"])
            wcs.append(c)
        # Land onto a scratch ref so main is unchanged for other runs.
        tip = json.load(urllib.request.urlopen(self.tiers.server + "/v1/resolve?rev=main"))["id"]
        ref = f"bench-land-{int(time.time())}"
        req = urllib.request.Request(self.tiers.server + "/v1/refs/update", data=json.dumps({"name": ref, "old": "", "new": tip}).encode(),
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req).read()
        attempts = [0] * agents

        def gft_agent(a):
            c = wcs[a]
            for k in range(per_agent):
                p = os.path.join(c, f"svc{a:02d}/mod0/pkg0/file00.txt")
                with open(p, "a") as fh:
                    fh.write(f"agent {a} change {k}\n")
                out = run([GFT, "commit", "-m", f"agent {a} change {k}", "--land", ref], cwd=c).stdout.decode()
                m = re.search(r"(\d+) attempt", out)
                if not m:
                    raise RuntimeError("unexpected land output: " + out)
                attempts[a] += int(m.group(1)) - 1

        r = self.measure("land", f"gft commit --land (server-side rebase + CAS)", lambda i: go(gft_agent), reps=1, server=True)
        for a in range(agents):
            body = run([GFT, "cat", "--server", self.tiers.server, "--rev", ref, f"svc{a:02d}/mod0/pkg0/file00.txt"]).stdout.decode()
            assert all(f"agent {a} change {k}\n" in body for k in range(per_agent)), f"gft lost agent {a} changes"
        depth = json.load(urllib.request.urlopen(f"{self.tiers.server}/v1/resolve?rev={ref}"))["depth"]
        base = json.load(urllib.request.urlopen(f"{self.tiers.server}/v1/resolve?rev=main"))["depth"]
        assert depth - base == agents * per_agent, "gft history should be linear with one changeset per land"
        print("    verified: all changes present, linear history")

        r["lands"] = agents * per_agent
        r["retries"] = sum(attempts)
        r["lands_per_s"] = r["lands"] / r["wall_s"]
        print(f"    -> {r['lands_per_s']:.1f} lands/s, {r['retries']} CAS retries (handled server-side)")

    def fleet_land(self, agents=32, per_agent=4):
        print(f"\n[land-fleet] {agents} agents x {per_agent} lands: 1 server vs 3 storage shards + 3 API servers", flush=True)
        results = {}
        for mode in ("single", "fleet"):
            work = self.path("fleet-" + mode)
            shutil.rmtree(work, ignore_errors=True)
            os.makedirs(work)
            procs = []
            def start(args, url):
                procs.append(subprocess.Popen([GFT] + args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
                for _ in range(200):
                    try:
                        urllib.request.urlopen(url + "/v1/info").read()
                        return
                    except Exception:
                        time.sleep(0.05)
            if mode == "single":
                start(["serve", "--tier", "server", "--data", os.path.join(work, "d"), "--listen", "127.0.0.1:7480"], "http://127.0.0.1:7480")
                apis = ["http://127.0.0.1:7480"]
            else:
                nodes = ",".join(f"http://127.0.0.1:749{i}" for i in range(3))
                for i in range(3):
                    start(["serve", "--tier", "store", "--data", os.path.join(work, f"s{i}"), "--nodes", nodes, "--shard", str(i),
                           "--listen", f"127.0.0.1:749{i}"], f"http://127.0.0.1:749{i}")
                napi = int(os.environ.get("FLEET_APIS", "3"))
                peers = ",".join(f"http://127.0.0.1:750{i}" for i in range(napi))
                for i in range(napi):
                    start(["serve", "--tier", "server", "--store", nodes, "--peers", peers, "--self", str(i),
                           "--listen", f"127.0.0.1:750{i}"], f"http://127.0.0.1:750{i}")
                apis = [f"http://127.0.0.1:750{i}" for i in range(napi)]
            run([GFT, "write", "--server", apis[0], "--parent", "", "--land", "main", "-m", "init", "README=hello"])
            def agent(a):
                api = apis[a % len(apis)]
                for k in range(per_agent):
                    run([GFT, "write", "--server", api, "--parent", "main", "--land", "main", "-m", f"agent {a} change {k}",
                         f"agents/{a}/{k}.txt=change {k}"])
            ts = [threading.Thread(target=agent, args=(a,)) for a in range(agents)]
            t0 = time.perf_counter()
            for t in ts:
                t.start()
            for t in ts:
                t.join()
            wall = time.perf_counter() - t0
            log = json.load(urllib.request.urlopen(apis[-1] + "/v1/resolve?rev=main"))
            assert log["depth"] == agents * per_agent, f"{mode}: lost lands ({log['depth']})"
            r = {"scenario": "land-fleet", "system": f"gft {mode} ({len(apis)} API server(s))", "wall_s": wall, "cpu_s": 0,
                 "lands": agents * per_agent, "retries": 0, "lands_per_s": agents * per_agent / wall, "reps": 1}
            self.results.append(r)
            print(f"  {r['system']:<34} {wall*1000:10.1f} ms   {r['lands_per_s']:.1f} lands/s (verified linear)", flush=True)
            cpu = 0.0
            for p in procs:
                out = subprocess.run(["ps", "-o", "time=", "-p", str(p.pid)], capture_output=True, text=True).stdout.strip()
                if out:
                    m, sec = out.split(":")[-2:]
                    cpu += int(m) * 60 + float(sec)
            r["cpu_s"] = cpu
            print(f"    server CPU {cpu:.2f} s ({cpu * 1000 / r['lands']:.1f} ms per land)", flush=True)
            for p in procs:
                p.send_signal(signal.SIGTERM)
            for p in procs:
                p.wait()

    def footprint(self):
        def du(p):
            return int(run(["du", "-sk", p]).stdout.split()[0]) * 1024
        self.results.append({"scenario": "storage", "system": "git bare repo (aggressively packed)", "bytes": du(self.git)})
        self.results.append({"scenario": "storage", "system": "gft server store", "bytes": du(self.args.gft)})
        print(f"\n[storage] git {du(self.git)/1e6:.0f} MB, gft {du(self.args.gft)/1e6:.0f} MB")

    def write(self):
        out = os.path.join(ROOT, "out")
        os.makedirs(out, exist_ok=True)
        meta = {"git_version": run(["git", "--version"]).stdout.decode().strip(),
                "gft_version": run([GFT, "version"]).stdout.decode().strip(),
                "machine": run(["uname", "-mrs"]).stdout.decode().strip(),
                "cpus": os.cpu_count(), "date": time.strftime("%Y-%m-%d"),
                "repo": os.path.basename(self.git), "reps": self.reps}
        with open(os.path.join(out, "results.json"), "w") as f:
            json.dump({"meta": meta, "results": self.results}, f, indent=2)
        lines = [f"# Benchmark results ({meta['date']})", "",
                 f"{meta['machine']}, {meta['cpus']} CPUs; {meta['git_version']}; {meta['gft_version']}; repo `{meta['repo']}`; median of {meta['reps']} runs unless noted.",
                 "", "| scenario | system | wall | CPU (client+server) | notes |", "|---|---|---:|---:|---|"]
        for r in self.results:
            if "bytes" in r:
                lines.append(f"| {r['scenario']} | {r['system']} | | | {r['bytes']/1e6:.0f} MB |")
                continue
            note = ""
            if "lands_per_s" in r:
                note = f"{r['lands_per_s']:.1f} lands/s, {r['retries']} retries"
            lines.append(f"| {r['scenario']} | {r['system']} | {fmt(r['wall_s'])} | {fmt(r['cpu_s'])} | {note} |")
        with open(os.path.join(out, "RESULTS.md"), "w") as f:
            f.write("\n".join(lines) + "\n")
        print("\nwrote bench/out/RESULTS.md")


def fmt(s):
    return f"{s*1000:.0f} ms" if s < 10 else f"{s:.1f} s"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--git", required=True)
    ap.add_argument("--gft", required=True)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    b = Bench(a)
    try:
        steps = ["checkout_sparse", "checkout_full", "mount_read", "history_setup", "update", "status", "history", "concurrent_land", "fleet_land", "footprint"]
        only = set(a.only.split(",")) if a.only else None
        for s in steps:
            if s == "history_setup":
                if not only or only & {"update", "status", "history"}:
                    b.setup_clones()
                continue
            if only and s not in only:
                continue
            getattr(b, s)()
        b.write()
    finally:
        b.tiers.stop()
        shutil.rmtree(b.work, ignore_errors=True)


if __name__ == "__main__":
    main()
