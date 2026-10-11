---
id: TASK-1.48
title: >-
  Rename the feature from "hybrid BGSAVE" to "hybrid snapshot": configs
  hybrid-snapshot*, module option HANDLE_HYBRID_SNAPSHOT, docs, tests, log
  lines, bench scripts
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-11 08:17'
updated_date: '2026-10-11 08:35'
labels:
  - hybrid-bgsave
  - naming
milestone: m-1
dependencies: []
references:
  - src/config.c (four createConfig entries)
  - src/redismodule.h REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE
  - >-
    src/module.c moduleHybridBgsaveBlockedBy /
    moduleAllModulesHandleHybridBgsave
  - 'tests/unit/snapshot.tcl, tests/unit/memefficiency.tcl, tests/helpers'
  - 'utils/snap-bench/*.sh, bench.py'
  - 'docs/hybrid-bgsave.md, docs/images/hybrid-bgsave-flow.png'
parent_task_id: TASK-1
priority: medium
ordinal: 44000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Decided with the user on 2026-10-11: "hybrid" still names the mechanism exactly (a parent thread serializes one part of the keyspace, a fork child the other), but "BGSAVE" no longer names the scope: the same path serves BGSAVE, disk and diskless full sync and the AOF rewrite, and one config and one module option gate all three. The feature is renamed "hybrid snapshot" before any release ships the config names. Scope: configs bgsave-hybrid -> hybrid-snapshot, bgsave-hybrid-max-retained-bytes -> hybrid-snapshot-max-retained-bytes, bgsave-hybrid-max-queued-bytes -> hybrid-snapshot-max-queued-bytes, bgsave-hybrid-tag-all -> hybrid-snapshot-tag-all, with the server fields and redis.conf; module option REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE -> REDISMODULE_OPTIONS_HANDLE_HYBRID_SNAPSHOT and the module.c identifiers; the phrase in comments, log lines, test names and docs (docs/hybrid-bgsave.md -> docs/hybrid-snapshot.md with its figure); the test helpers; the snap-bench scripts that pass the config. Code identifiers with the neutral 'snap' prefix, INFO field names, the poc spec (history), the backlog files and the user's blog draft are not touched.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 No 'bgsave-hybrid', 'bgsave_hybrid', 'HYBRID_BGSAVE' or 'hybrid BGSAVE' (any case) remains in src, tests, redis.conf, docs or utils/snap-bench; the poc spec, backlog and .mfiles are the only places the old name survives
- [x] #2 redis.conf documents the four hybrid-snapshot* configs; redismodule.h documents HANDLE_HYBRID_SNAPSHOT; docs/hybrid-snapshot.md exists and every reference points to it
- [x] #3 Build without warnings; redis-server test snapshot; unit/snapshot with hybrid-snapshot yes and no, unit/aofrw, integration/aof-multi-part, integration/replication and unit/info with the hybrid on all pass
- [x] #4 The memory notes that quote the config name are updated
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
A scripted sweep over git-tracked files in src, tests, redis.conf, docs, utils/snap-bench (59 files) plus modules/vector-sets/vset.c (which uses the module option and broke the build until swept), with ordered rules: the three long config names first, then bgsave-hybrid, bgsave_hybrid, HYBRID_BGSAVE, HybridBgsave, the test helper wait_hybrid_bgsave_done, the doc and figure file names, the snapshot.h title (WHAT IS A HYBRID SNAPSHOT?), then the phrase in its three capitalizations. git mv for docs/hybrid-snapshot.md and docs/images/hybrid-snapshot-flow.png. Residual matches of the old name exist only in tests/tmp residue (ignored), the poc spec, backlog files and .mfiles, by design. Grammar of the rewritten phrases sampled in redis.conf, snapshot.h, server.c: reads naturally ("A hybrid snapshot (hybrid-snapshot yes)", "Hybrid snapshot:" log prefix). The user allows the short form "hybrid snap" where brevity helps (recorded in memory feature-naming.md); not needed anywhere in this pass. Memory note shell-and-test-pitfalls.md updated to the new runtest config. Scratch scripts in the session scratchpad still pass bgsave-hybrid and must be adjusted before reuse (tsan-loop*.sh, san-final.sh, fullsuite.sh).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
The feature is now "hybrid snapshot" everywhere that ships: four hybrid-snapshot* configs in config.c and redis.conf, REDISMODULE_OPTIONS_HANDLE_HYBRID_SNAPSHOT in redismodule.h and the modules, docs/hybrid-snapshot.md with its figure, and the phrase in comments, log lines and test names. Code identifiers keep the snap prefix and INFO fields are unchanged; the poc spec, backlog history and the user's blog draft keep the old name. Verified with the new config name across unit/snapshot (both modes), aofrw, aof-multi-part, replication, info and memefficiency, all green, plus a clean build and the C snapshot tests.
<!-- SECTION:FINAL_SUMMARY:END -->
