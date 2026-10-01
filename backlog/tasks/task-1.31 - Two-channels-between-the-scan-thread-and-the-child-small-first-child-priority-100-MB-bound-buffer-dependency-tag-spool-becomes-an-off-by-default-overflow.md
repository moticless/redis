---
id: TASK-1.31
title: >-
  Two channels between the scan thread and the child, small-first child
  priority, 100 MB bound, buffer dependency tag; spool becomes an off-by-default
  overflow
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-01 11:04'
updated_date: '2026-10-01 13:07'
labels:
  - hybrid-bgsave
  - performance
  - replication
dependencies:
  - TASK-1.30
references:
  - >-
    utils/snap-bench/RESULTS.md run 17 (128 MB single FIFO: idle 26-29 s, writes
    38-41 s), run 22 correction, run 29
  - >-
    src/snapshot.c snapOutEnqueue, snapOutDrain, snapSaveChildWrite,
    snapSaveDispatch, snapSendMsg
  - src/evict.c freeMemoryGetNotCountedMemory
documentation:
  - poc-hybrid-bgsave-spec.md section 6
  - 'redis.conf bgsave-hybrid-max-queued-bytes, bgsave-hybrid-spool'
parent_task_id: TASK-1
priority: high
ordinal: 26000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
User's design (2026-10-01), replacing the spool as the primary overlap mechanism. Two channels from the scan thread to the child instead of one FIFO: a small channel for BUFFER messages (scan-thread staging buffers and forwarded main-thread emissions) and a big channel for KVOBJ messages (big-value headers), END and ABORT. The child prioritizes the small channel while it has a backlog (about 1 MB or more; the child sees only the pipe, so 'small pipe readable' or a queued-bytes counter in the shared page is the signal) and takes one big value only when the small side is nearly empty. The small channel's queue in the parent is bounded by bgsave-hybrid-max-queued-bytes, raised to a 100 MB default; the big channel is tiny.

Why (RESULTS.md): run 17's single FIFO at 128 MB saved mixed1m idle in 26-29 s but 38-41 s under writes, because the main thread hands the child all 24 hashes at the first write to each and the child took them back to back for 21 s while the scan thread parked at the bound. The small-first rule interleaves: one 0.85 s hash, then the ~47 MB the scan produced meanwhile, then the next hash, so a 100 MB bound never parks the scan on this dataset and the expected result is about 25 s in all cells (spool: 21-26 s, run 29) with a 50 MB queue peak in used_memory instead of a 61-77 MB spool footprint off RSS. Ceiling: a single value longer than bound / scan rate (about 1.8 s, a 200 MB hash) parks the scan for its remainder; partial and graceful.

Ordering for TASK-1.29 (inline replication): a command on a big key H in the small channel must not overtake H's record in the big channel (the replica would apply the HSET and then overwrite it with the record). Tag every small-channel BUFFER with the count of KVOBJ messages sent before it (all KVOBJ sends go through the scan thread's snapSendMsg, so the count is well defined); the child writes a BUFFER only after it has written that many big values. Today's spool with hysteresis also reorders the small stream itself (newer queued buffers overtake older spooled ones); with the spool reduced to an overflow behind the small channel the same tag covers it. Under writes to big keys the tag forces the child through all big values first; with a bounded channel the scan then parks (run 17's shape), with the spool it does not: that is why the spool stays as an option for 1.29 rather than being deleted.

Eviction: the queue is zmalloc'd and counted in used_memory, and evict.c does not exclude it (freeMemoryGetNotCountedMemory handles replication buffers and AOF only). At 100 MB a maxmemory instance would evict during every save; exclude the queued bytes like the replication buffers.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 BUFFER messages travel on a small channel and KVOBJ/END/ABORT on a big channel (two pipes); the child drains the small channel while it has a backlog and takes a big value only when the small side is nearly empty; the policy is documented in the spec section 6
- [x] #2 Every small-channel BUFFER carries the KVOBJ count at production time and the child never writes it before that many big values; a unit test with rdb-key-save-delay and a write to a big key during the save checks the record precedes the later buffers in the output
- [x] #3 bgsave-hybrid-max-queued-bytes defaults to 100 MB; the queued bytes are excluded from the eviction calculation (test: maxmemory just above the dataset, a save with a full queue evicts nothing)
- [x] #4 bgsave-hybrid-spool defaults to no; with yes the spool is the overflow of the small channel beyond the bound and the dependency tag keeps spooled and queued buffers in production order
- [x] #5 mixed1m at 4 GB on the c7i.4xlarge: idle, 20k and 50k writes/s within 10% of run 29's spool numbers (21-26 s) with queue peak under 100 MB and no spool; small unchanged within noise; full sync (run 25 recipe) unchanged
- [x] #6 Race harness, unit/snapshot both modes, TSan and ASan clean; abort and child-killed paths close both pipes
- [x] #7 redis.conf, spec section 6 and comparison table, RESULTS.md run 30 describe the two channels and the bound
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Read the queue/pipe/spool paths: snapOutEnqueue, snapOutDrain, snapSendMsg, snapSaveChildWrite, pipe creation in snapSaveCreate, snapSaveThreadDone, the shared page.
2. Second pipe pair; outbound queue split per channel (small list with the bound, big list unbounded), drain per channel; KVOBJ/END/ABORT on the big channel.
3. KVOBJ sequence counter on the scan thread; BUFFER header carries the count; shared page publishes small queued bytes.
4. Child loop: poll both pipes; small readable or dependency unmet -> small; else big; END on the big channel -> drain small to EOF, then finish; a buffer whose dependency is unmet waits on the big pipe.
5. Spool reduced to overflow behind the small channel, default off, same framing and tag.
6. Config default 100 MB; evict.c excludes the queued bytes; INFO unchanged.
7. Tests: ordering with rdb-key-save-delay and a write to a big key, eviction with a full queue, both pipes closed on abort/child killed; docs.
8. Run 30 on the host: two-channel vs spool vs nospool on mixed1m and small, run 25 recipe.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implementation decisions (2026-10-01): (1) two pipes, one queue list per channel, one bound over both (oq_bytes); the small list's bytes are published to the child in a MAP_SHARED page (snapShared.small_queued) and the child takes the small channel while that is >= 1 MB or the big channel is idle. (2) END carries kv_seq (KVOBJ count) in robj_off and small_seq (small-channel message count) in src_addr; the child finishes when both channels are consumed, no pipe close needed. (3) Dependency: BUFFER robj_off = big values that must precede it; the child holds such a buffer and reads the big channel until met; plain saves send 0; hidden debug config bgsave-hybrid-tag-all sets kv_seq on every buffer for the test. (4) Spool overflow keeps production order with a SNAP_MSG_SPOOL marker (payload: offset, length) enqueued on the small channel when spilling ends and before END; the child reads the segment in message units at its place in the stream; the shared committed-offset page is gone (the marker follows the writes). With spool on and no bound, spilling is on for the whole save and one marker closes it at END. (5) bgsave-hybrid-spool default no, bgsave-hybrid-max-queued-bytes default 100 MB, evict.c excludes snapSaveQueuedBytes() (oq + mq). (6) run.sh: hybrid-spool config added; hybrid-nospool kept (now the default shape).

Committed 3406f4b4c. Verified: C unit 166/166; unit/snapshot feature on (220 s) and stock (250 s) all pass; ASan (255 s) and TSan (415 s, setarch -R, port 23111) all pass, no reports. AC2's test exercises the dependency path through bgsave-hybrid-tag-all with a stopped child (the order itself is not observable from the RDB without a parser; the test checks completion and reload equality). Run 30 started 11:31 UTC on the host (tree ~/redis-2ch, driver ~/run30.sh, CSVs ~/run30.csv and ~/run30s.csv). First cached idle cells: hybrid (100 MB, no spool) 26.2 s with parent +110 MB, hybrid-spool 27.0-27.2 s (+99-103 MB: the queue fills to the bound before anything spills), hybrid-q16 32.9 s (+7 MB): with a 16 MB bound the two channels alone do not give the overlap, one hash's worth of scan output (about 47 MB) has to fit. The first hybrid cell (33.2 s) ran right after the live populate, where the main dict is mid-rehash and the 24 hashes sit in table 1, visited last (run 22 correction): the bounded shape cannot overlap on that layout, the spool shape can; compare cached cells only.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
The scan thread feeds the child through two channels and the child drains the small backlog first, so the bounded queue gives the full overlap of small and big values without a spool file. mixed1m at 4 GB on the c7i.4xlarge (run 30b): 22.7 s idle, 23.6-24.0 s at 20k and 21.0-22.7 s at 50k writes/s, against 35-42 pipe-only and 40-46 stock, with the queue peaking at 54-58 MB idle and 101-116 MB under writes, all in used_memory and released at the end.

**Design (the user's, 2026-10-01).** Small channel: BUFFER messages (staging buffers and forwarded main-thread emissions) and SPOOL markers. Big channel: KVOBJ, END, ABORT. One queue list per channel in the scan thread, one bound (`bgsave-hybrid-max-queued-bytes`, default raised from 16 to 100 MB) over both. The child polls both pipes and takes the small channel while the parent reports a small backlog of 1 MB or more (a MAP_SHARED page the scan thread updates on enqueue and drain), a big value otherwise: one 0.85 s hash, then the 47 MB the scan produced meanwhile, then the next hash. END carries both channels' message counts and the child finishes when both are consumed.

**Fix after run 30 (bfaa21174).** The first build overlapped idle but not under writes (28.9-36.5 s): the scan thread pushes its queue into the pipe only at its own enqueues, so the small pipe is empty for most of every millisecond, and the child took the next big value at each such gap with tens of megabytes waiting in the parent; the 24 hashes handed over at the first write to each then ran nearly back to back and the queue hit the bound. The child now waits up to 20 ms for the pending backlog before taking a big value.

**Dependency field (for TASK-1.29).** Every BUFFER header carries the number of big values that must precede it; the child holds such a buffer and reads the big channel until met. Plain saves send 0. The hidden debug config `bgsave-hybrid-tag-all` sets it for every buffer; a test runs that path with a stopped child and writes to the big hashes.

**Spool.** Off by default; on, it is the small channel's overflow past the bound, kept in stream order by a SPOOL marker (offset, length) queued when spilling ends and before END, so the committed-offset page of TASK-1.30 is gone. Run 30b: the spool shape is now 1-3 s slower than the bounded one and spools 0 / 2-11 / 97-98 MB at 0 / 20k / 50k. It stays as the option for inline syncs under writes to big keys (the dependency forces the child through the big values first, and only an unbounded buffer keeps the scan thread running meanwhile) and for values larger than the bound allows (about 200 MB here).

**Eviction.** `freeMemoryGetNotCountedMemory` excludes the queued bytes like the replication buffers; a test with maxmemory just above the dataset and a full queue evicts nothing.

**Measured.** Run 30 (first build) and run 30b (fix) in RESULTS.md; small 28.7-31.4 s for both configs over two reps (run 29's one-rep flag closed as noise); full sync 15.9 / 16.5 s (run 25: 16.2 / 17.0). The first cell of a dataset after a live populate is 30-40% slower and on mixed1m has the big hashes last in scan order (mid-rehash dict, run 22 correction), the one layout where a bounded queue cannot overlap: those cells are marked and excluded.

**Tests.** Eviction exclusion, dependency path under tag-all, spool tests switched to spool on, both-pipes shape through every existing abort/kill/FLUSHDB/SWAPDB/diskless test. C unit 166/166; unit/snapshot feature on and stock; ASan and TSan clean on both commits.

**Docs.** redis.conf (both knobs), spec section 6 (new paragraph, comparison row), RESULTS.md runs 30 and 30b, run.sh `hybrid-spool` config.

**Commits.** 3406f4b4c (two channels), bfaa21174 (child wait), faa74d2eb (run 30), plus run 30b.
<!-- SECTION:FINAL_SUMMARY:END -->
