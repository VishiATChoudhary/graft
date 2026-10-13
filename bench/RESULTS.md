# Benchmark results (2026-10-13)

Darwin 25.1.0 arm64, 10 CPUs; git version 2.50.1 (Apple Git-155); gft 0.1.0; repo `mono.git`; median of 5 runs unless noted.

| scenario | system | wall | CPU (client+server) | notes |
|---|---|---:|---:|---|
| checkout-sparse | git clone --filter=blob:none --sparse | 1743 ms | 2625 ms |  |
| checkout-sparse | git ... + --depth 1 (best case) | 716 ms | 425 ms |  |
| checkout-sparse | gft clone --sparse (cold, no daemon) | 54 ms | 55 ms |  |
| checkout-sparse | gft clone --sparse (warm node daemon) | 66 ms | 49 ms |  |
| checkout-full | git clone | 10.2 s | 11.4 s |  |
| checkout-full | git clone --depth 1 (best case) | 9291 ms | 9939 ms |  |
| checkout-full | gft clone (cold, no daemon) | 4523 ms | 9084 ms |  |
| checkout-full | gft clone (cold daemon, warm regional cache) | 4928 ms | 10.4 s |  |
| checkout-full | gft clone (warm node daemon) | 4918 ms | 9867 ms |  |
| update-100 | git checkout | 190 ms | 567 ms |  |
| update-100 | gft checkout | 68 ms | 95 ms |  |
| status | git status | 249 ms | 630 ms |  |
| status | gft status | 120 ms | 706 ms |  |
| blame | git blame (local full clone) | 124 ms | 97 ms |  |
| blame | gft blame (first query, derives) | 26 ms | 29 ms |  |
| blame | gft blame (memoized) | 20 ms | 25 ms |  |
| log-file | git log -- <file> | 77 ms | 67 ms |  |
| log-file | gft log <file> | 12 ms | 17 ms |  |
| log-dir | git log -n 50 -- svc03/ | 11 ms | 9 ms |  |
| log-dir | gft log -n 50 svc03 | 6 ms | 10 ms |  |
| is-ancestor | git merge-base --is-ancestor | 11 ms | 8 ms |  |
| is-ancestor | gft is-ancestor | 6 ms | 10 ms |  |
| ci-diff | git diff --name-only (in full clone) | 11 ms | 9 ms |  |
| ci-diff | gft diff --stat (API, no checkout) | 7 ms | 12 ms |  |
| land | git commit + push (retry: pull --rebase) | 41.2 s | 225.9 s | 3.1 lands/s, 439 retries |
| land | gft commit --land (server-side rebase + CAS) | 751 ms | 2259 ms | 170.4 lands/s, 580 retries |
| storage | git bare repo (aggressively packed) | | | 106 MB |
| storage | gft server store | | | 479 MB |
