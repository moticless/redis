---
id: TASK-1.42
title: >-
  Hybrid BGSAVE design document: current state only, with the lifecycle of one
  save, the funnel cases, bounds, entry points, interactions, module contract,
  observability and glossary
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-09 22:23'
updated_date: '2026-10-09 23:05'
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
- [x] #1 The document contains all ten parts (a) to (j) listed in the description, with no dates and no task IDs
- [x] #2 At least one diagram: the blog's figure 1 reused, plus a channel and queue diagram (producers, mq, oq/bq, two pipes, child)
- [x] #3 The four decisions that live only in backlog notes (small-channel wait and END counts, drain dict-type swap, expiry policy during a save, no generation tag) are stated in the document
- [x] #4 poc-hybrid-bgsave-spec.md either becomes the history appendix or is replaced by a pointer to the new document; no remaining section describes the temp-dict design as current
- [x] #5 redis.conf, redismodule.h and the snapshot.c header point to the document
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Sources: spec §0-§7 for the problem, classes, window protocol, channels and bounds; 11.z.2 for the scan/main-thread agreement and the ASM-trim and rehash rules; 11.z.4 for the module contract; snapshot.c header (TASK-1.40) for the lifecycle; server.c INFO blocks and snapshot.c serverLog lines for observability; debug.c for the DEBUG subcommands; backlog notes TASK-1.31 (small-channel wait, END counts, drain on abort), TASK-1.37 (drain's dict-type swap), TASK-1.9 (expiry policy during a save), TASK-1.21.6 (no generation tag) for the decisions that exist only there; this session's TASK-1.15, 1.37, 1.39 outcomes for the entry points and the interactions table; blog figure 1 (images/hybrid-bgsave-flow.png) for the diagram.
2. Write docs/hybrid-bgsave.md with the ten parts (a)-(j) in the acceptance criteria, current state only, no dates, no task IDs, with an ASCII channel/queue diagram and the blog figure referenced.
3. Replace the top of poc-hybrid-bgsave-spec.md with a pointer to the new document and a statement that the rest is the design history; keep its content for the record.
4. Point the snapshot.c header, redis.conf (bgsave-hybrid entry) and redismodule.h (the option's comment) at docs/hybrid-bgsave.md.
5. Verify every INFO field named in the document exists in server.c (grep), every config exists in config.c, and the interactions table agrees with the tests in unit/snapshot.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Written 2026-10-09/10 as docs/hybrid-bgsave.md (391 lines) from spec §0-§7, 11.z.2, 11.z.4, the snapshot.c header, server.c INFO blocks, snapshot.c log lines, debug.c, and the notes of the four tasks named in the description. Verified by grep that every INFO field and config the document names exists in server.c and config.c and that the document holds no dates or task IDs. The figure is copied to docs/images/. The spec is kept as the history with a pointer at the top; its content is otherwise unchanged (its stale sections are named in the pointer).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
docs/hybrid-bgsave.md is the reference for the hybrid BGSAVE as implemented, written for a Redis core reviewer who has not seen the branch. Ten parts: problem and the two classes with the arena and placement; one save end to end as a main-thread / scan-thread / child sequence, with the flow figure and an ASCII diagram of the queues and the two channels; the write funnel (behind, emitted, inside, ahead) with the race-freedom argument, attach and pause, the side table's four chains, clone, drain and release, and the reference-count model; the memory model and its two bounds; every entry point and whether it takes the hybrid or the stock child; an interactions table (FLUSHALL, FLUSHDB, SWAPDB, DEBUG RELOAD/LOADAOF, RM_ResetDataset, full-sync load, ASM trim, rehash, defrag, expiry, eviction, SHUTDOWN, child crash, hard mark, CONFIG SET) with a same-as-stock column; the module contract including the stored-pointer and main-thread rules; every config, INFO field, DEBUG subcommand and log line; a glossary; eight invariants. The four decisions that lived only in task notes are stated: the child's small-channel policy and END's counts, the drain's private dict type, the expiry policy during a save, and why no leftover handling exists.

poc-hybrid-bgsave-spec.md is kept as the design history with a pointer to the document at the top and its superseded sections named. snapshot.c, snapshot.h, redis.conf and the REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE comment point to the document; the option's comment now also carries the stored-pointer and main-thread rules. No dates and no task IDs in the document; every INFO field and config it names was checked against server.c and config.c.
<!-- SECTION:FINAL_SUMMARY:END -->
