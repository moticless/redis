---
id: TASK-1.28
title: >-
  Place client reply/query buffers and replication buffer blocks in the DONTFORK
  arena; measure with a large backlog under a heavy write stream
status: To Do
assignee: []
created_date: '2026-09-28 16:02'
labels:
  - hybrid-bgsave
  - memory
dependencies: []
references:
  - >-
    utils/snap-bench/RESULTS.md: Copy-on-write sources other than big values
    (cow-sources.py)
  - .mfiles/bgsave-blog/bgsave-blog-draft.md section 4
  - src/networking.c createClient / addReplyProtoToList
  - src/replication.c feedReplicationBuffer / createReplicationBacklog
  - src/server.c dismissMemoryInChild / dismissClientMemory
parent_task_id: TASK-1
priority: medium
ordinal: 23000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
cow-sources.py (RESULTS.md 2026-09-26) showed no copy-on-write beyond the 3 MB child floor from a replica, AOF or 64 pipelined clients at 20k writes/s with the default 1 MB backlog. Two buffers scale beyond that and are rewritten in place during every save: client reply buffers (c->buf, 1-16 KB per client, resized by clientsCron; plus clientReplyBlock list blocks for clients with pending output; plus private query buffers of clients mid-command) at 10k active clients are 100-160 MB, and the replication backlog / replica output ring (replBufBlock, shared by the backlog and all replicas) is rewritten end to end when the write stream during the save exceeds repl-backlog-size: up to the backlog size (hundreds of MB to 1 GB on busy primaries) of CoW per save. None is read by any fork child (RDB child writes the file; diskless-sync child writes the RDB it generates; AOF rewrite child reads the dataset only), and with the TASK-1.22 flip stock children map the arena anyway.

Work:
1. Allocation sites to route through zmalloc_nofork_usable / nofork sds: createClient (c->buf), the clientReplyBlock allocator (addReplyProtoToList), replBufBlock creation (feedReplicationBuffer / createReplicationBacklog), the private client querybuf (sds; the thread-shared query buffer can stay), optionally server.aof_buf. Frees and reallocs are already range-aware.
2. dismissMemoryInChild -> dismissClientMemory: skip the madvise(DONTNEED) of buffers that are unmapped in a hybrid child (same condition as the kvstore dismissal in server.c: snapArenaAvailable() && !snapArenaForkable()); a failing madvise is harmless (ENOMEM) but should not be attempted.
3. Placement follows the bgsave-hybrid gate like values do (zmalloc_nofork_enabled): with the feature off the layout is stock.
4. Measure before/after with snap-bench on mixed (or small) at 4 GB, 50k writes/s, with a replica attached and repl-backlog-size 512mb, and separately with 5k-10k connected clients (snapload --conns, or a pipelined idle-client generator); record child Private_Dirty and extra_mem_peak_mb. Add a 'clients' knob or dataset variant to the harness if needed.
5. Update blog section 4 and RESULTS.md with the numbers.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 With a replica attached, repl-backlog-size 512mb and a write stream during the save larger than the backlog, the hybrid child's Private_Dirty stays within a few MB of the idle-save floor (before the change it is up to the backlog size)
- [ ] #2 With thousands of connected active clients, reply and query buffers add no child copy-on-write (before: about 16 KB per active client)
- [ ] #3 INFO memory allocator_allocated_nofork grows by the buffers' size when the feature is on; layout is stock when it is off
- [ ] #4 dismissMemoryInChild skips unmapped buffers in the hybrid child; the stock child (arena forkable) still dismisses them
- [ ] #5 Full suite with --config bgsave-hybrid yes passes; replication and AOF tests unaffected
- [ ] #6 RESULTS.md and blog section 4 updated with before/after numbers
<!-- AC:END -->
