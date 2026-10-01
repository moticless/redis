---
id: TASK-1.23
title: >-
  Harness scenarios adopted from the teammate's POC: address reuse, read-path
  mutations, failed publication, blocked-helper cancel
status: Done
assignee:
  - '@Claude'
created_date: '2026-09-24 06:21'
updated_date: '2026-10-01 15:04'
labels:
  - bgsave
  - testing
milestone: m-3
dependencies: []
documentation:
  - poc-hybrid-bgsave-spec.md
  - tests/unit/snapshot.tcl
parent_task_id: TASK-1
priority: medium
ordinal: 12200
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec 11.z.3, item 3: corner cases the teammate's tcl suites exercise that our randomized harness does not reach. Add to tests/unit/snapshot.tcl, each racing a slow hybrid save and checking the reload digest against the fork instant: (1) address reuse: delete keys during the save and create new ones until a new kvobj lands at a freed address (verify with DEBUG OBJECT or a debug command exposing the address); (2) in-place RAW string mutation from a read command (PFCOUNT on a HyperLogLog) and XREADGROUP updating consumer-group state through a read lookup; (3) failed publication: kill the child after it has written part of the file and assert the previous dump.rdb is untouched and loadable; (4) cancellation while the scan thread is blocked on a full pipe (child stopped with SIGSTOP), then FLUSHALL / SHUTDOWN NOSAVE, asserting no hang and a clean next save. The module-metadata case is already covered (commit 4f0046b69).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Each of the four scenarios exists as a test in tests/unit/snapshot.tcl and passes on the jemalloc build and under TSan in arena-free mode
- [x] #2 The address-reuse test demonstrates an actual reuse (asserts the address matched at least once) rather than hoping for it
- [x] #3 The failed-publication test asserts the old dump file's digest is unchanged after the failed save
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-01: four scenarios written into tests/unit/snapshot.tcl (harness block): (1) address reuse: 3000 same-size strings, slow scan, DEL + SET of a same-size key in creation order until DEBUG OBJECT shows the new key at the deleted key's address while the save runs (keys behind the scan are freed at once and the arena's tcache hands the slot back; owed keys are retained so their address cannot come back); reload equals the fork-instant digest; reuse found within the first few hundred keys (230 ms). (2) PFCOUNT on 300 sparse HLLs and one dense one plus XREADGROUP on 100 streams during a slow save: the live digest changes, the reload equals the fork instant. THIS FOUND A BUG: PFCOUNT writes its cardinality cache into the value after a read lookup, so during a save it wrote into a frozen original (debug assertion in dbUnshareStringValueByLink fired, the child-side read would have been torn); fixed in hyperloglog.c by taking the key through the write funnel (lookupKeyWrite clones it into the overlay) when snapKvobjIsFrozenOriginal says so. XREADGROUP edits a SNAP_FORK value in place, which is allowed (the child has its CoW copy). (3) failed publication: a good dump, then a second save whose child is killed -9 once temp-<pid>.rdb exceeds 100 KB: status err, the dump's md5 unchanged, the temp file removed, DEBUG RELOAD NOSAVE loads the old dump. (4a) FLUSHALL with the child SIGSTOPped and the scan thread at a 1 MB bound returns in under 3 s (the bound wait polls the abort flag every 100 ms), the next save works. (4b) SHUTDOWN NOSAVE in the same state: the server exits, no crash; the test sends SHUTDOWN from a deferring client because the stopped child inherits the client socket and the reply never comes (the first version hung there for 17 minutes and left stopped children behind; several such orphans from today's runs were killed by hand). All six pass alone; the whole unit is rerunning.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Four harness scenarios from the teammate's POC are in tests/unit/snapshot.tcl (commit b89355ece), each racing a slow hybrid save and checking the reload against the fork-instant digest; one of them found a real bug.

- **Address reuse (asserted).** 3000 same-size strings; during the save, DEL then SET of a same-size key in creation order until DEBUG OBJECT shows the new key at the deleted key's address while the save is still running (keys behind the scan are freed at once and the arena's tcache hands the slot straight back; owed keys are retained, so their address cannot come back). The reload equals the fork instant.
- **Read commands that write the value in place.** PFCOUNT on 300 sparse HLLs and one dense one, XREADGROUP on 100 streams with a consumer group, during a slow save; the live digest changes, the reload equals the fork instant. **Bug found:** PFCOUNT wrote its cardinality cache into a frozen original after a read lookup (the debug assertion in dbUnshareStringValueByLink fired; in a release build the scan thread could have read a torn or post-fork value). Fix in hyperloglog.c: when snapKvobjIsFrozenOriginal says the key is owed, PFCOUNT re-fetches it with lookupKeyWrite, which clones it into the overlay, before the unshare and the cache write. XREADGROUP edits a SNAP_FORK stream in place, which the design allows (the child has its copy-on-write view).
- **Failed publication.** A good dump, then a save whose child is killed -9 once temp-<pid>.rdb exceeds 100 KB: status err, the dump's md5 unchanged, the temp file removed, DEBUG RELOAD NOSAVE loads the old dump to the old digest.
- **Cancellation at the queue bound.** Child SIGSTOPped, 1 MB bound, scan thread in its bound wait: FLUSHALL returns in under 3 s (the wait polls the abort flag every 100 ms), the next save works; SHUTDOWN NOSAVE in the same state exits cleanly with no crash report. The shutdown test sends SHUTDOWN from a deferring client, because a stopped fork child inherits the client's socket and the reply never arrives (the first version hung on it).

Verified: feature-on unit (221 s) and stock unit (228 s) all pass; TSan arena-free (459 s) all pass, no reports; ASan run in progress at the time of writing (the previous ASan on the same tree was clean).
<!-- SECTION:FINAL_SUMMARY:END -->
