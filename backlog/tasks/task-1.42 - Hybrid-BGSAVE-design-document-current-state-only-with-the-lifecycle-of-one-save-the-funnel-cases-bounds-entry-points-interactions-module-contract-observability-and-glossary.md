---
id: TASK-1.42
title: >-
  Hybrid BGSAVE design document: current state only, with the lifecycle of one
  save, the funnel cases, bounds, entry points, interactions, module contract,
  observability and glossary
status: To Do
assignee: []
created_date: '2026-10-09 22:23'
labels:
  - hybrid-bgsave
  - docs
milestone: m-1
dependencies: []
documentation:
  - poc-hybrid-bgsave-spec.md
  - .mfiles/bgsave-blog/bgsave-blog-draft.md
  - utils/snap-bench/SUMMARY.md
parent_task_id: TASK-1
priority: high
ordinal: 38000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The 2026-10-09 review found that a Redis core reviewer cannot get the overall picture or the flow of one hybrid save from the existing documents: poc-hybrid-bgsave-spec.md is 57% dated history (sections 8 through 11.z.1 and 11.z.3 are instructions, superseded designs or status reports), its current sections still describe the per-slot temp dict and merge, it has no end-to-end sequence of a save and no diagram, vocabulary (owed, fork instant, emit-ahead, window, retained, queued, hard mark, store vs db) is defined nowhere in one place, and several decisions exist only in backlog task notes: the child's 20 ms small-channel wait and END's two counts (TASK-1.31), the drain's dict-type swap (TASK-1.37), the expiry policy during a save (TASK-1.9), why no generation tag is needed (TASK-1.21.6). The blog's section 3 with its figure is the clearest existing explanation of the window.

Outcome wanted: one design document (docs/hybrid-bgsave.md or equivalent) describing the current state only, no dates and no task IDs, that a reviewer reads before the code. Parts: (a) problem and the two classes with the arena backing; (b) lifecycle of one save as a numbered sequence with a main-thread / scan-thread / child column: sizes captured, freeze with references and guard, fork, thread start, window publish and quiesce, BUFFER and KVOBJ on two channels, the child's small-first ordering and the robj_off dependency, END counts, child done, stock done handler; (c) the write funnel's three cases and the side table's drain and release; (d) memory model and bounds: arena, retained vs queued, hard mark, worst case as the sum of the two bounds, eviction exclusion; (e) entry points: BGSAVE and save triggers, disk and diskless sync with and without the rdb channel, AOF rewrite in both formats, functions-only, ASM slot snapshot, module and Lua debugger forks and the arena flip, CONFIG SET of the switch; (f) interactions table: FLUSHALL, FLUSHDB, SWAPDB, DEBUG RELOAD/LOADAOF, RM_ResetDataset, full-sync load, ASM trim, rehash and resize, defrag, expiry and field expiry, eviction, SHUTDOWN, child crash; (g) module contract including the stored-pointer and main-thread rules redismodule.h omits; (h) observability: every INFO field, log lines, DEBUG subcommands, debug config; (i) glossary; (j) invariants. The spec's history moves to an appendix or stays in the backlog. The code's file header and banners point to the document (TASK-1.40 fixes the banners themselves).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The document contains all ten parts (a) to (j) listed in the description, with no dates and no task IDs
- [ ] #2 At least one diagram: the blog's figure 1 reused, plus a channel and queue diagram (producers, mq, oq/bq, two pipes, child)
- [ ] #3 The four decisions that live only in backlog notes (small-channel wait and END counts, drain dict-type swap, expiry policy during a save, no generation tag) are stated in the document
- [ ] #4 poc-hybrid-bgsave-spec.md either becomes the history appendix or is replaced by a pointer to the new document; no remaining section describes the temp-dict design as current
- [ ] #5 redis.conf, redismodule.h and the snapshot.c header point to the document
<!-- AC:END -->
