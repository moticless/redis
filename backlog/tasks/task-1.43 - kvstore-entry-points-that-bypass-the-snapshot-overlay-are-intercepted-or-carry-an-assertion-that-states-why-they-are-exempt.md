---
id: TASK-1.43
title: >-
  kvstore entry points that bypass the snapshot overlay are intercepted or carry
  an assertion that states why they are exempt
status: To Do
assignee: []
created_date: '2026-10-09 22:23'
updated_date: '2026-10-10 08:02'
labels:
  - hybrid-bgsave
  - robustness
milestone: m-1
dependencies:
  - TASK-1.41
references:
  - >-
    src/kvstore.c kvstoreDictFetchValue (calls dictFetchValue directly),
    kvstoreDictSetKey, kvstoreDictSetVal, kvstoreDictScanDefrag,
    kvstoreDictLUTDefrag (replaces dicts[didx] while the save's table holds the
    old pointer), kvstoreTryResizeDicts, kvstoreIncrementallyRehash,
    kvstoreGetDictMeta, kvstoreGetDict (raw bypass used by snapshot.c and
    cluster_asm.c:3106), kvstoreExpand (tests pauseAutoResize instead of
    ovFrozen)
  - >-
    src/defrag.c:1997, 2157 (defrag skipped while a child exists), the unstated
    fact the bypasses rely on
parent_task_id: TASK-1
priority: medium
ordinal: 39000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The 2026-10-09 review found the kvstore interception uniform for the fifteen keyspace read and write entry points but incomplete: the entry points listed in the references reach the dict without consulting the overlay. They are safe today only because their callers are not keyspace paths (pubsub, bless, defrag) and because active defrag is skipped while a child process exists, and nothing in kvstore states either fact. Outcome wanted: every kvstore entry point that touches dict content either goes through the same two-line interception as the others or carries a debug-build assertion that the dict is not frozen, with a comment stating the exemption, so that a future caller cannot silently bypass a running save.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Each entry point in the references is either intercepted with the standard ovRead/ovWrite/ovFrozen pattern or asserts not-frozen in debug builds with a one-line reason
- [ ] #2 kvstoreExpand uses the frozen predicate rather than the pause flag
- [ ] #3 A debug-build test demonstrates that calling an exempt entry point on a frozen keyspace dict trips the assertion
- [ ] #4 unit/snapshot, redis-server test snapshot and the defrag tests of unit/memefficiency pass with the hybrid on
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Premise to verify before any code (2026-10-10): this task was filed from a reviewer's claim that kvstoreDictFetchValue, kvstoreDictSetKey/SetVal and the defrag entry points bypass the overlay unsafely. Their callers were not audited when the task was filed. First step is to list every caller of each listed entry point and decide, per entry point, whether a keyspace path can reach it during a save; an entry point no keyspace path reaches needs only the assertion, and if none can, this task may close with assertions alone or as not needed.
<!-- SECTION:NOTES:END -->
