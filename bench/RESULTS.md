# Benchmark results (2026-10-13)

Darwin 25.1.0 arm64, 10 CPUs; git version 2.50.1 (Apple Git-155); gft 0.1.0; repo `mono.git`; median of 5 runs unless noted.

| scenario | system | wall | CPU (client+server) | notes |
|---|---|---:|---:|---|
| checkout-sparse | git clone --filter=blob:none --sparse | 1175 ms | 2430 ms |  |
| checkout-sparse | git ... + --depth 1 (best case) | 315 ms | 307 ms |  |
| checkout-sparse | gft clone --sparse (cold, no daemon) | 26 ms | 30 ms |  |
| checkout-sparse | gft clone --sparse (warm node daemon) | 26 ms | 31 ms |  |
| checkout-full | git clone | 9220 ms | 10.9 s |  |
| checkout-full | git clone --depth 1 (best case) | 8848 ms | 9703 ms |  |
| checkout-full | gft clone (cold, no daemon) | 5751 ms | 12.2 s |  |
| checkout-full | gft clone (cold daemon, warm regional cache) | 4511 ms | 9791 ms |  |
| checkout-full | gft clone (warm node daemon) | 4667 ms | 9470 ms |  |
| mount-read | gft mount + read package (warm daemon) | 93 ms | 70 ms |  |
| update-100 | git checkout | 149 ms | 576 ms |  |
| update-100 | gft checkout | 50 ms | 78 ms |  |
| status | git status | 136 ms | 579 ms |  |
| status | gft status | 99 ms | 699 ms |  |
| status-watched | git status (core.fsmonitor daemon) | 27 ms | 14 ms |  |
| status-watched | gft status (gft watch) | 25 ms | 17 ms |  |
| blame | git blame (local full clone) | 92 ms | 66 ms |  |
| blame | gft blame (first query, derives) | 81 ms | 58 ms |  |
| blame | gft blame (memoized) | 8 ms | 12 ms |  |
| log-file | git log -- <file> | 54 ms | 52 ms |  |
| log-file | gft log <file> | 8 ms | 12 ms |  |
| log-dir | git log -n 50 -- svc03/ | 10 ms | 8 ms |  |
| log-dir | gft log -n 50 svc03 | 6 ms | 10 ms |  |
| is-ancestor | git merge-base --is-ancestor | 11 ms | 9 ms |  |
| is-ancestor | gft is-ancestor | 7 ms | 13 ms |  |
| grep | git grep -F (in full clone) | 2135 ms | 11.2 s |  |
| grep | gft grep -F (server-side, no clone) | 73 ms | 295 ms |  |
| ci-diff | git diff --name-only (in full clone) | 11 ms | 9 ms |  |
| ci-diff | gft diff --stat (API, no checkout) | 6 ms | 10 ms |  |
| land | git commit + push (retry: pull --rebase) | 40.3 s | 222.9 s | 3.2 lands/s, 455 retries |
| land | gft commit --land (server-side rebase + CAS) | 485 ms | 2275 ms | 263.7 lands/s, 0 retries |
| land-fleet | gft single (1 API server(s)) | 228 ms | 0 ms | 4495.1 lands/s, 0 retries |
| land-fleet | gft fleet (3 API server(s)) | 271 ms | 0 ms | 3774.3 lands/s, 0 retries |
| storage | git bare repo (aggressively packed) | | | 106 MB |
| storage | gft server store | | | 97 MB |
