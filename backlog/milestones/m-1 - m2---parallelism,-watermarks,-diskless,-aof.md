---
id: m-1
title: "M2 - Parallelism, watermarks, diskless, AOF"
---

## Description

Hybrid BGSAVE milestone 2 (poc-hybrid-bgsave-spec.md §9): multiple background threads over disjoint slots; per-slot merge with mid-save frees; soft/hard memory watermarks; diskless full sync; AOF rewrite over the same scan.
