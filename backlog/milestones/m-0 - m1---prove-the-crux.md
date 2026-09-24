---
id: m-0
title: "M1 - Prove the crux"
---

## Description

Hybrid BGSAVE spike, milestone 1 (poc-hybrid-bgsave-spec.md §9). One background thread, slots scanned sequentially, all frees deferred to save end. Deliverables: (a) RDB that reloads to a keyspace identical to stock BGSAVE on a static dataset; (b) under concurrent writes across every write path, reload-and-diff equals the fork-instant reference; (c) FLUSHALL/FLUSHDB/SWAPDB abort cleanly; (d) no CoW on kvobj headers, SNAP_NOFORK values or hash tables. Stop and report after M1. Do NOT build multi-thread slot parallelism, mid-scan frees, diskless, or AOF in M1.
