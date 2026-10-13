# Benchmark results (2026-10-13)

Darwin 25.1.0 arm64, 10 CPUs; git version 2.50.1 (Apple Git-155); gft 0.1.0; repo `mono.git`; median of 5 runs unless noted.

| scenario | system | wall | CPU (client+server) | notes |
|---|---|---:|---:|---|
| checkout-sparse | git clone --filter=blob:none --sparse | 1174 ms | 2464 ms |  |
| checkout-sparse | git ... + --depth 1 (best case) | 308 ms | 302 ms |  |
| checkout-sparse | gft clone --sparse (cold, no daemon) | 26 ms | 30 ms |  |
| checkout-sparse | gft clone --sparse (warm node daemon) | 25 ms | 29 ms |  |
| checkout-full | git clone | 9364 ms | 10.9 s |  |
| checkout-full | git clone --depth 1 (best case) | 8998 ms | 9614 ms |  |
| checkout-full | gft clone (cold, no daemon) | 5074 ms | 11.8 s |  |
| checkout-full | gft clone (cold daemon, warm regional cache) | 4702 ms | 9746 ms |  |
| checkout-full | gft clone (warm node daemon) | 4852 ms | 9923 ms |  |
| mount-read | gft mount + read package (warm daemon) | 86 ms | 66 ms |  |
| update-100 | git checkout | 148 ms | 603 ms |  |
| update-100 | gft checkout | 52 ms | 78 ms |  |
| status | git status | 147 ms | 623 ms |  |
| status | gft status | 99 ms | 708 ms |  |
| status-watched | git status (core.fsmonitor daemon) | 26 ms | 13 ms |  |
| status-watched | gft status (gft watch) | 25 ms | 17 ms |  |
| blame | git blame (local full clone) | 91 ms | 64 ms |  |
| blame | gft blame (first query, derives) | 108 ms | 90 ms |  |
| blame | gft blame (memoized) | 8 ms | 12 ms |  |
| log-file | git log -- <file> | 54 ms | 51 ms |  |
| log-file | gft log <file> | 9 ms | 13 ms |  |
| log-dir | git log -n 50 -- svc03/ | 9 ms | 8 ms |  |
| log-dir | gft log -n 50 svc03 | 6 ms | 10 ms |  |
| is-ancestor | git merge-base --is-ancestor | 10 ms | 8 ms |  |
| is-ancestor | gft is-ancestor | 5 ms | 9 ms |  |
| grep | git grep -F (in full clone) | 2071 ms | 11.8 s |  |
| grep | gft grep -F (server-side, no clone) | 64 ms | 279 ms |  |
| ci-diff | git diff --name-only (in full clone) | 11 ms | 9 ms |  |
| ci-diff | gft diff --stat (API, no checkout) | 7 ms | 11 ms |  |
| land | git commit + push (retry: pull --rebase) | 35.8 s | 224.2 s | 3.6 lands/s, 442 retries |
| land | gft commit --land (server-side rebase + CAS) | 653 ms | 2349 ms | 196.1 lands/s, 206 retries |
| land-fleet | gft single (1 API server(s)) | 640 ms | 0 ms | 200.1 lands/s, 0 retries |
| land-fleet | gft fleet (3 API server(s)) | 923 ms | 0 ms | 138.7 lands/s, 0 retries |
| storage | git bare repo (aggressively packed) | | | 106 MB |
| storage | gft server store | | | 97 MB |
