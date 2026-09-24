---
id: TASK-1.21.6
title: Extend the bucket window scheme to cluster mode (per-slot dicts)
status: To Do
assignee: []
created_date: '2026-09-21 19:57'
updated_date: '2026-09-21 19:58'
labels:
  - bgsave
  - cluster
milestone: m-3
dependencies:
  - TASK-1.21.5
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1.21
priority: medium
ordinal: 11960
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
After the standalone spike: the window W = (dict, table, [lo, hi)) already names the dict, so in cluster mode it walks the 16,384 slot dicts in order and a window often covers a whole small dict. This task makes membership checks, resize lockout (per dict, only while that dict is under the window or still unwalked), the side-table drain and the walk-to-completion rule work over kvstore slot dicts, keeps SLOT_INFO records correct when the main thread emits keys for a slot the walker has not reached, and revisits the M1 note that empty slots are DONE at freeze (no longer a concept). Cluster atomic slot migration during a save stays refused or guarded as today; document which.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The cluster variant of the race harness (start_cluster 1 0) reloads equal to the fork instant, including keys emitted by the main thread for slots the walker had not reached
- [ ] #2 SLOT_INFO records in the hybrid RDB are accepted by redis-check-rdb and a stock cluster node loads the file
- [ ] #3 Resize lockout applies per slot dict and dicts already walked resume resizing before the save ends
- [ ] #4 Slot migration during a save is either refused with a clear error or verified safe by a test; the choice is documented in the spec
<!-- AC:END -->
