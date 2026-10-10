---
id: TASK-1.43
title: >-
  kvstore entry points that bypass the snapshot overlay are intercepted or carry
  an assertion that states why they are exempt
status: To Do
assignee: []
created_date: '2026-10-09 22:23'
updated_date: '2026-10-10 08:14'
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

Caller audit (2026-10-10), every entry point the task names: kvstoreDictFetchValue: pubsub.c only (pubsub_channels, pubsubshard_channels), never a keyspace store. kvstoreDictSetKey: defrag.c on pubsub_channels only. kvstoreDictSetVal: pubsub.c (channel client dicts), bless.c on db->blessed_keys (a store the save never freezes: the save scans db->keys only), defrag.c on pubsub_channels. kvstoreDictScanDefrag and kvstoreDictLUTDefrag: defrag.c only, and active defrag does not run while a child process exists (defrag.c, stock rule), so neither can meet a frozen dict or the save's dict table. kvstoreTryResizeDicts and kvstoreIncrementallyRehash: cron on the keyspace stores, but both act through dict-level operations that honor the pause of the dict under the window and the rehash hooks of attached dicts (stage B); they are not a bypass. kvstoreGetDictMeta: slot statistics metadata, no dict content. kvstoreExpand: db.c dbExpand at load and full sync, on fresh kvstores after the detach (never a frozen one); its pauseAutoResize test is equivalent to ovFrozen for the one paused dict but says less. Conclusion: the premise 'unsafe bypass' does not hold today; no keyspace path reaches an unguarded entry point during a save. What remains worth doing is the defensive part: a debug-build assertion (not frozen) with a one-line reason at FetchValue, SetKey, SetVal, ScanDefrag and LUTDefrag, kvstoreExpand on ovFrozen, and a comment in defrag.c naming the child-exists rule the two defrag entry points rely on. AC #3 (a test that trips the assertion) is not writable in-process since debugServerAssert panics; propose dropping it. Scope down to assertions and comments, about an hour, after the full-suite run.
<!-- SECTION:NOTES:END -->
