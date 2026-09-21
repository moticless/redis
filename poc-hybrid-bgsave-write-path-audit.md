# Write-path audit for hybrid BGSAVE

Tree: `/home/moti/git/redis-bgsave` branch `snapshot-hybrid-bgsave` (kvobj keyspace, listpackEx, keymeta.c, hotkeys.c, t_array.c, gcra.c). All line numbers are from this tree as read on 2026-09-20. `snapshot.c` currently contains the SNAP_NOFORK arena, class-aware free wrappers and a scan-thread skeleton (`snapScanFreezeStore` snapshot.c:347 = `dictPauseRehashing`+`dictPauseAutoResize`; `snapScanKeyIsOwed(ctx, store, didx)` snapshot.c:508; `snapScanEntryCb` snapshot.c:370 runs `dictScan` on the bg thread). **No clone-on-write hook is wired into db.c/object.c yet** — this document enumerates where they must go.

Spec invariant under audit (poc-hybrid-bgsave-spec.md §4/§7.1): while a key's slot is PENDING/SCANNING the main thread must never write in place into (a) that key's kvobj allocation (metadata slots + robj + embedded key), (b) a SNAP_NOFORK value, (c) the slot's `db->keys` / `db->expires` dict. SNAP_FORK values may be mutated in place.

Two structural facts that colour everything below:

1. **The robj header is one 32-bit bitfield word** (`object.h:100-112`: `type:4, encoding:4, refcount, iskvobj:1, metabits:8, lru:24`) followed by `ptr`. Any write to `refcount`, `lru`, `encoding`, `type` or `metabits` is a read-modify-write of the same word the bg thread reads for `type/encoding/lru`. There is no field that can be treated as an independently "word-sized atomic" store except `ptr` and the 8-byte metadata slots that precede the robj (`kvobjMetaRef` object.c:27).
2. **The hash-field-expiration `ExpireMeta` of a LISTPACK_EX hash lives inside the VALUE allocation** (`listpackEx { ExpireMeta meta; void *lp; }` server.h:3792-3799; `hashGetExpireMeta` t_hash.c:3737-3750). The db-level `subexpires` ebuckets link through `lpt->meta` (`next` pointer, `trash`, segment flags, expire time — ebuckets.c:217-282, 377-392, 540-619, 691-706). So `estoreAdd/Remove/Update` (estore.c:129/145/169) and `ebSetMetaExpTime` (expire.c:191) **write into a SNAP_NOFORK value in place**, and the RDB serializer **reads exactly that field** (`rdbSaveObject` rdb.c:1240-1241 → `hashTypeGetMinExpire(o,0)` t_hash.c:2018-2042 reads `lpt->meta.trash` and the packed expire time). This is the single most dangerous non-obvious path (see §7).

---

## 1. Shared primitives

Legend for "mutates": HDR = kvobj header word/`ptr`; META = metadata slots before the robj; VAL = value buffer; KEYS = `db->keys` dict; EXP = `db->expires` dict; HFE = `db->subexpires` ebuckets (and therefore `lpt->meta` / dict-metadata ExpireMeta inside the value); IDMP = `db->stream_idmp_keys`. Thread: M = main only, unless stated.

| # | Primitive (file:line) | Signature | Mutates | Notes / thread |
|---|---|---|---|---|
| P1 | `lookupKey` db.c:279-345 | `kvobj *lookupKey(redisDb*, robj *key, int flags, dictEntryLink *link)` | HDR.lru (db.c:319-324 via `updateLFU` db.c:55-59 or `val->lru = LRU_CLOCK()` db.c:322); may DELETE via `expireIfNeeded` (db.c:301). Also a plain `dbFindByLink`→`dictFindLink`→`_dictRehashStepIfNeeded` (dict.c:1729) = KEYS rehash step unless paused. | M. **Already skipped when `hasActiveChildProcess()`** (db.c:319). The hybrid save still forks, so `server.child_pid != -1` (server.c:842) and the LRU/LFU write is suppressed for the whole save. `LOOKUP_NOTOUCH` also skips. Every `lookupKeyRead*/Write*` (db.c:349-393), `kvobjCommandLookup` (object.c:1692), `RM_OpenKey` (module.c:4181-4184) funnel here. |
| P2 | `expireIfNeeded` db.c:2935-3006 | `keyStatus expireIfNeeded(redisDb*, robj *key, kvobj *kv, int flags)` | → `deleteExpiredKeyAndPropagate` db.c:2826 → `deleteKeyAndPropagate` db.c:2775 → `dbGenericDelete` (db.c:2807) | M. Called from P1, `scanCallback` db.c:1703, `dbRandomKey` db.c:833, `delGenericCommand` db.c:1419, `keysCommand` (db.c:1583ff). Deletion is suppressed when `isPausedActionsWithUpdate(PAUSE_ACTION_EXPIRE)` (db.c:2993). |
| P3 | `dbAddInternal` db.c:428-457 | `kvobj *dbAddInternal(redisDb*, robj *key, robj **valref, dictEntryLink *link, const KeyMetaSpec*)` | Allocates new kvobj (`kvobjSet` db.c:434 — see P12), KEYS insert (`kvstoreDictSetAtLink(...,newItem=1)` db.c:436 → `dictSetKeyAtLink` dict.c:876 → **`dictExpandIfNeeded`** dict.c:891), EXP insert via `setExpireByLink` (db.c:443), META memcpy (db.c:450), keysizes/allocsize kvstore metadata (db.c:455-457). | M. Wrappers: `dbAdd` db.c:454, `dbAddByLink` db.c:460. Callers: every "create key" path in t_*.c, RENAME/MOVE/COPY/RESTORE, `setKeyByLink`. |
| P4 | `dbAddRDBLoad` db.c:518-555 | `kvobj *dbAddRDBLoad(redisDb*, sds key, robj **valref, const KeyMetaSpec*)` | same as P3 without signals | M (loading: DEBUG RELOAD/LOADAOF, replica full sync, ASM import? — import uses RESTORE-ASKING via fake CLIENT_MASTER client cluster_asm.c:1439/2511, i.e. P3). |
| P5 | `dbSetValue` db.c:578-713 (static) | `void dbSetValue(redisDb*, robj *key, robj **valref, dictEntryLink link, int overwrite, int updateKeySizes, int keepTTL)` | HFE (`estoreRemove(db->subexpires,…,old)` db.c:593 — writes old value's ExpireMeta if hash), IDMP (db.c:596), **HDR.refcount of old** (`incrRefCount(old)` db.c:614 / `decrRefCount(old)` db.c:627 on overwrite), META callbacks (`keyMetaOnUnlink` db.c:618), then EITHER **in-place header swap** (db.c:636-652: `old->type/encoding/ptr = val->…` — the ORIGINAL kvobj header is rewritten and kept in the dict; only the *value* object is freed) and `removeExpire` (db.c:651, EXP delete + `kvobjSetExpire(kv,-1)` in place), OR new-kvobj path (`kvobjSet` db.c:657 → steals `val->ptr`, `kvstoreDictSetAtLink(...,0)` db.c:658 rewrites the dict entry, `kvobjSetExpire(kvNew)` db.c:663, EXP entry rewrite db.c:667 or delete db.c:669, `keyMetaTransition(old,kvNew)` db.c:674 **writes reset values into OLD's metadata slots** keymeta.c:708). Frees old via `tryDeferFreeClientObject` (IO thread) db.c:705 / `freeObjAsync` (bio) db.c:707 / `decrRefCount` db.c:709. | M (+ frees on IO/bio threads). Wrappers: `dbReplaceValue` db.c:715, `dbReplaceValueWithLink` db.c:723, `setKeyByLink` db.c:754-798 (`setKey` db.c:742). |
| P6 | `dbGenericDelete` db.c:844-901 | `int dbGenericDelete(redisDb*, robj *key, int async, int flags)` | KEYS (`kvstoreDictTwoPhaseUnlinkFind` db.c:848 / `…Free` db.c:891 or `kvstoreDictSetAtLink(NULL)` db.c:889), EXP (`kvstoreDictDelete` db.c:882), HFE (`estoreRemove` db.c:858 — writes value ExpireMeta), IDMP (db.c:862), **HDR.refcount** (`incrRefCount(kv)` db.c:866, `decrRefCount(kv)` db.c:875), META callbacks (db.c:868), module unlink notify (db.c:870, may re-enter via RM_StringDMA — see comment db.c:864), free (`freeObjAsync` db.c:887 → bio thread, or sync). | M. Wrappers: `dbSyncDelete` db.c:903, `dbAsyncDelete` db.c:909, `dbDelete` db.c:915, `dbDeleteSkipKeysizesUpdate` db.c:925, `deleteKeyAndPropagate` db.c:2775 (expire/evict), `expireGenericCommand` db.c-expire.c:807, `getexCommand` t_string.c:530, `increxCommand` t_string.c:1334, `asmActiveTrimDeleteKey` cluster_asm.c:3658, `RM_DeleteKey/UnlinkKey` module.c:4282/4296, `moduleDelKeyIfEmpty` module.c:790. |
| P7 | `setExpireByLink` db.c:2711-2760 | `kvobj *setExpireByLink(client*, redisDb*, sds key, long long when, dictEntryLink keyLink)` | If already expirable: **META write in place** (`kvobjSetExpire` db.c:2723 → object.c:277 `*kvobjMetaRef(kv,0)=expire`). Else: HFE remove (db.c:2733, value write), **realloc kvobj** (`kvobjSetExpire`→`kvobjSet` object.c:272 — steals `kv->ptr` from OLD header: `val->ptr = NULL` object.c:303, then `decrRefCount(val)` object.c:328 frees the OLD header), KEYS entry rewrite (db.c:2738), EXP insert (`kvstoreDictAddRaw` db.c:2745 → `dictAddRaw` → expand/rehash step), HFE re-add (db.c:2749, value write). | M. Wrapper `setExpire` db.c:2706. Callers: EXPIRE family expire.c:820, SET EX/PX t_string.c:184, GETEX t_string.c:538, MSETEX t_string.c:882, INCREX t_string.c:1374, GCRA gcra.c:214/271, RESTORE via P3, `RM_SetExpire/SetAbsExpire` module.c:4328/4360, `dbAddInternal` db.c:443. |
| P8 | `removeExpire` db.c:2685-2697 | `int removeExpire(redisDb*, robj *key)` | **META write in place** (`kvobjSetExpire(kv,-1)` db.c:2692), EXP delete (`kvstoreDictTwoPhaseUnlinkFree` db.c:2694) | M. Callers: PERSIST expire.c:914, GETEX PERSIST t_string.c:548, INCREX PERSIST t_string.c:1371, `dbSetValue` db.c:651, `RM_SetExpire(NO_EXPIRE)` module.c:4330/4362. |
| P9 | `kvobjSetExpire` object.c:264-279 | `kvobj *kvobjSetExpire(kvobj*, long long expire)` | META slot 0 write (object.c:277) or realloc via `kvobjSet` (object.c:272) | M. Called by P5/P7/P8. |
| P10 | `kvobjSet` object.c:282-330 | `kvobj *kvobjSet(sds key, robj *val, uint32_t keyMetaBits)` | Allocates new kvobj (`kvobjCreate` object.c:62 / `kvobjCreateEmbedString` object.c:163); **mutates the source object**: `val->ptr = NULL` (object.c:303) when refcount==1, `keyMetaTransition(val,kv)` (object.c:325, writes val's META), `decrRefCount(val)` (object.c:328 — frees val if it was the old kvobj). | M. This is the function that, when `val` is the *existing* kvobj (P7 realloc path, `keyMetaSetMetadata`, RENAME/MOVE with refcount juggling), nulls and frees the fork-instant header. |
| P11 | `keyModified` db.c:1165-1171 → `updateLRM` db.c:62-68 | `void keyModified(client*, redisDb*, robj *key, robj *val, int signal)` | HDR.lru (`o->lru = LRU_CLOCK()` db.c:66) **only when `maxmemory_policy & MAXMEMORY_FLAG_LRM`**; NOT gated by `hasActiveChildProcess`. | M. Called at the end of virtually every write command with the kvobj pointer (e.g. t_string.c:641, 1436; t_hash.c:2550; module.c:4217 `moduleCloseKey`; hyperloglog.c:1782 PFCOUNT). |
| P12 | `dbUnshareStringValueByLink` db.c:963-971 | `kvobj *dbUnshareStringValueByLink(redisDb*, robj *key, kvobj *o, dictEntryLink link)` | If shared or not RAW: creates RAW copy, `dbReplaceValueWithLink` (P5). If already RAW & refcount 1: returns the same kvobj, and **the caller then writes `kv->ptr` / the sds in place** (APPEND t_string.c:1429, SETRANGE t_string.c:637-640, SETBIT bitops.c:803, PFADD hyperloglog.c:1659ff, PFCOUNT hyperloglog.c:1749-1777, `RM_StringDMA` module.c:4746-4749, `RM_StringTruncate` module.c:4776-4787). | M. Wrapper `dbUnshareStringValue` db.c:957. |
| P13 | `tryObjectEncodingEx` object.c:926-984 | `robj *tryObjectEncodingEx(robj*, int try_trim)` | **In-place** RAW→INT (`sdsfree(o->ptr); o->encoding=INT; o->ptr=value` object.c:953-957); RAW trim `o->ptr = sdsRemoveFreeSpace` (object.c:920 via `trimStringObjectIfNeeded`). | M. In this tree it is only ever applied to `argv` objects and at RDB load (rdb.c:2429) — never to a kvobj already in the dict (grep confirmed: no non-argv callers outside object.c/rdb.c). `RM_TrimStringAllocation` also only touches argv (module.c:980). NOOP for the invariant unless a future caller passes a kvobj. |
| P14 | `hashTypeConvert` t_hash.c:1749 → `hashTypeConvertListpack` t_hash.c:1632-1687, `hashTypeConvertListpackEx` t_hash.c:1689-1747 | `void hashTypeConvert(redisDb*, robj *o, int enc)` | **HDR.encoding + HDR.ptr in place** (t_hash.c:1653-1654, 1681-1682, 1738-1739); frees old value (`zfree(o->ptr)` t_hash.c:1680, `listpackExFree` t_hash.c:1735); HFE remove/add (t_hash.c:1702, 1742 — value ExpireMeta writes); for LISTPACK→LISTPACK_EX rewrites the listpack in place first (`lpInsertInteger` t_hash.c:1647). | M. Callers: `hashTypeTryConversion` t_hash.c:609-639 (HSET/HSETNX/HMSET/HSETEX pre-check), `hashTypeSet` t_hash.c:926/959/1002, `hashTypeSetExInit` t_hash.c:1215 (LISTPACK→LISTPACK_EX on first HEXPIRE/HSETEX/HGETEX), `hashTypeDup` (COPY, on a fresh object), rdb load. |
| P15 | `setTypeConvertAndExpand` t_set.c:507 (via `setTypeConvert` t_set.c:499, `setTypeMaybeConvert` t_set.c:46, `maybeConvertIntset` t_set.c:63, `maybeConvertToIntset` t_set.c:72) | `int setTypeConvertAndExpand(robj *setobj, int enc, unsigned long cap, int panic)` | HDR.encoding/ptr in place (t_set.c:534-535, 559-560, 584, 591) | M. |
| P16 | `zsetConvertAndExpand` t_zset.c:1444 (via `zsetConvert` t_zset.c:1439, `zsetTypeMaybeConvert` t_zset.c:1428, `zsetConvertToListpackIfNeeded` t_zset.c:1523) | `void zsetConvertAndExpand(robj *zobj, int encoding, unsigned long cap)` | HDR.ptr/encoding in place (t_zset.c:1489-1490, 1513-1514) | M. |
| P17 | `listTypeTryConversion` t_list.c:130 (→ `listTypeTryConvertListpack` t_list.c:23-55, `listTypeTryConvertQuicklist` t_list.c:67-95) | `void listTypeTryConversion(robj *o, list_conv_type lct, beforeConvertCB fn, void *data)` | HDR.ptr/encoding in place (t_list.c:53-54, 91-95) | M. Note the `beforeConvertCB` argument: callers hold listpack pointers/iterators across the conversion. |
| P18 | `hashTypeSet` t_hash.c:916-1065 | `int hashTypeSet(redisDb*, kvobj *o, sds field, sds value, int flags)` | VAL in place for listpack (`o->ptr = zl` t_hash.c:955) and listpackEx (`lpt->lp = lpReplace(...)` t_hash.c:972, `listpackExUpdateExpiry` t_hash.c:993, `listpackExAddNew` t_hash.c:998); HT: dict insert + per-field ebuckets `hfieldPersist`/`ebAdd` (t_hash.c:1040-1058); may call P14. | M. |
| P19 | `hashTypeDelete` t_hash.c:1286-1330 | `int hashTypeDelete(robj *o, void *field)` | VAL in place (`o->ptr = lpDeleteRangeWithEntry` t_hash.c:1300; `lpt->lp = …` t_hash.c:1314; HT `dictDelete` t_hash.c:1319) | M (also from lazy field expiry on READ, see §3). |
| P20 | `hashTypeSetExInit` t_hash.c:1202-1241 / `hashTypeSetEx` t_hash.c:1163-1192 / `hashTypeSetExDone` t_hash.c:1244-1284 | `int hashTypeSetExInit(robj *key, kvobj *o, client*, redisDb*, ExpireSetCond, HashTypeSetEx*)`, `SetExRes hashTypeSetEx(robj*, sds field, uint64_t expireAt, HashTypeSetEx*)`, `void hashTypeSetExDone(HashTypeSetEx*)` | Init: P14 LISTPACK→LISTPACK_EX (t_hash.c:1215) or `dictTypeAddMeta` + `o->ptr = ht` (t_hash.c:1221-1224, HDR.ptr in place). SetEx: `hashTypeSetExpiryListpack` t_hash.c:522 → `listpackExUpdateExpiry` t_hash.c:478 (`lpt->lp = lpDeleteRangeWithEntry` t_hash.c:502, `listpackExAddInternal` t_hash.c:517) = VAL in place; HT: `hashTypeSetExpiryHT` t_hash.c:1067. Done: HFE `estoreUpdate/Remove/Add` (t_hash.c:1276-1282) = **writes `lpt->meta` / dict-meta ExpireMeta**. | M. HEXPIRE family, HSETEX, HGETEX. |
| P21 | `hashTypeExpire` t_hash.c:1902-1986 | `uint64_t hashTypeExpire(redisDb*, kvobj *o, uint32_t *quota, int updateSubexpires, int activeEx)` | VAL (`listpackExExpire` t_hash.c:377-436: `lpt->lp = lpDeleteRange` t_hash.c:426; HT: `ebExpire`+`onFieldExpire`→P19 t_hash.c:3726), HFE (t_hash.c:1949/1959), may `dbDelete` (t_hash.c:1954). | M. Callers: active subexpire (`activeSubexpiresCb` expire.c:170-195 which also `ebSetMetaExpTime` on the value's ExpireMeta expire.c:191), `hashTypeExpireIfNeeded` t_hash.c:1988 (HRANDFIELD t_hash.c:3362/3633). |
| P22 | `estoreAdd/Remove/Update` estore.c:129/145/169; `estoreActiveExpire` estore.c:115; `estoreMoveEbuckets` estore.c:196 | `void estoreAdd(estore*, int eidx, eItem item, uint64_t when)` etc. | HFE ebuckets structure (rax/segments, own allocations) **and the item's ExpireMeta embedded in the hash VALUE** (ebuckets.c writes to `mItem->next`, `trash`, flags, expire time). | M. Callers: P5, P6, P7, P14, P20, P21, `keyMetaSetMetadata`, RENAME/MOVE/COPY, RESTORE, rdb load, ASM trim. |
| P23 | `keyMetaSetMetadata` keymeta.c:794-861 | `kvobj *keyMetaSetMetadata(redisDb*, kvobj *kv, KeyMetaClassId id, uint64_t metadata)` | META in place (keymeta.c:802) or **realloc kvobj** via `kvobjSet(key, kv, …)` (keymeta.c:838 — nulls old `ptr`, frees old header), KEYS entry rewrite (keymeta.c:842), META write (keymeta.c:846, 851), EXP entry rewrite (keymeta.c:852), HFE remove/add (keymeta.c:820/857). | M (module API `RM_SetKeyMeta` module.c:4605-4609, GIL-held). |
| P24 | `keyMetaTransition` keymeta.c:688-724 | `void keyMetaTransition(kvobj *kvOld, kvobj *kvNew)` | **writes reset_value into kvOld's META slots** (keymeta.c:708) | M. Called by P10 (object.c:325) and P5 (db.c:674). |
| P25 | `keyMetaOnUnlink` keymeta.c:292-325 / `keyMetaOnFree` keymeta.c:333-360 | | Module callbacks with pointers into META slots (`conf.unlink(&ctx, pMeta)` keymeta.c:315 may write the slot). OnFree may run on the **lazyfree thread** (comment keymeta.c:326-331). | M / bio. |
| P26 | `incrRefCount` / `decrRefCount` object.c:629-690 | | HDR.refcount bitfield RMW (object.c:631, 652); decrRefCount frees value+header (`zfree(alloc)` object.c:689) and calls `keyMetaOnFree`. | M, **bio** (`lazyfreeFreeObject` lazyfree.c:14-19), **IO threads** (`freeDeferredObject` networking.c:1713-1721 for RAW string values scheduled by db.c:705). |
| P27 | `kvstoreDictSetAtLink` kvstore.c:879 / `kvstoreDictAddRaw` kvstore.c:891 / `kvstoreDictDelete` kvstore.c:924 / `kvstoreDictTwoPhaseUnlinkFind/Free` kvstore.c:910/917 / `kvstoreDictFindLink` kvstore.c:859 / `kvstoreDictExpand` kvstore.c:~780 | | KEYS/EXP table structure; `dictSetKeyAtLink(newItem=1)` calls `dictExpandIfNeeded` (dict.c:891); every find performs `_dictRehashStepIfNeeded` (dict.c:1729) unless `pauserehash` (dict.c:1730). `dict_can_resize == DICT_RESIZE_AVOID` while a child exists (server.c:824) only *reduces* rehash, it does not forbid it (dict.c:477-486). | M. Direct callers outside db.c: defrag.c:1130/1132/1544/1546, keymeta.c:842/852, cluster_asm.c:2215-2216 (import pre-expand), rdb.c:4171-4172 (load pre-expand). |
| P28 | `kvstoreTryResizeDicts` kvstore.c:653 / `kvstoreIncrementallyRehash` kvstore.c:674 | | KEYS/EXP shrink/expand/rehash from cron (server.c:1340-1341, 1350-1353) | M. **Already gated by `!hasActiveChildProcess()`** (server.c:1326). |
| P29 | `objectSetLRUOrLFU` object.c:1660-1687 | `int objectSetLRUOrLFU(robj *val, long long lfu_freq, long long lru_idle, long long lru_clock, int lru_multiplier)` | HDR.lru (object.c:1665, 1682) | M. RESTORE IDLETIME/FREQ cluster.c:321, rdb load rdb.c:4404, `RM_SetLRU/SetLFU` module.c:14765/14792. |
| P30 | `activeDefragKvobj` defrag.c:1061-1100 / `defragKey` defrag.c:1102-1211 / `activeDefragSubexpiresOB` defrag.c:1522-1550 | | Relocates the whole kvobj allocation (`activeDefragAlloc(alloc)` defrag.c:1083), rewrites KEYS/EXP entries (defrag.c:1130-1132, 1544-1546), rewrites `ob->ptr` for every value type (defrag.c:1143-1195), relocates listpackEx struct **and** its ExpireMeta (defrag.c:1163-1167 + ebuckets `ebScanDefrag` defrag.c:1591 which rewrites `next` links) | M. **Already gated**: `activeDefragCycle` returns if `hasActiveChildProcess()` (defrag.c:1981) and the timer proc pauses (defrag.c:1834-1838). `defragWhileBlocked` defrag.c:1883 respects the same. |
| P31 | `emptyDbStructure`/`emptyData` db.c:973-1070, `emptyDbAsync` lazyfree.c:325-343, `dbSwapDatabases` db.c:2550-2598, `swapMainDbWithTempDb` db.c:2600-2644 | | Replace or free whole kvstores / estores; async variant hands the old kvstores to the **bio thread** (`kvsLazyfreeFree` lazyfree.c:55). | M + bio. |
| P32 | `kvstoreMoveDict` kvstore.c:557-588 / `estoreMoveEbuckets` estore.c:196 / `streamMoveIdmpKeys` db.c:1113 | | Detach a whole slot dict from `db->keys/expires/subexpires` (ASM background trim cluster_asm.c:3058-3061) then `emptyDbDataAsync` (cluster_asm.c:3065) → bio. | M + bio. |

---

## 2. Command families → primitives

"In-place VAL" = the command writes the value buffer directly after obtaining the kvobj (bypassing any db.c primitive); "HDR" = writes the header/ptr in place.

### Strings (t_string.c, bitops.c, hyperloglog.c)
| Command | Primitives | In-place mutations bypassing primitives |
|---|---|---|
| SET/SETNX/SETEX/PSETEX/GETSET/SET GET (`setGenericCommand` t_string.c:~100-190, `getsetCommand` 568) | P1 (t_string.c:104), P6 (`dbDelete` t_string.c:163 for SET with expired-past?), **P5 via `setKeyByLink`** (t_string.c:181), P7 (t_string.c:184), P11 | P5's header-swap path rewrites the OLD kvobj's `type/encoding/ptr` (db.c:636-652) when both old and new are RAW/non-embstr refcount 1 → **HDR write on the original**. |
| GETEX (t_string.c:499-556) | P1, P6 (t_string.c:530), P7 (538), P8 (548), P11 | — |
| GETDEL (t_string.c:557) | P1, P6 (`dbSyncDelete` 559) | — |
| MSET/MSETNX (t_string.c:756-817), MSETEX (821-890) | P5 via `setKey` (803, 877), P7 (882) | — |
| APPEND (t_string.c:1400-1442) | P1, P3 (1411), P12 (1426), P11 | `o->ptr = sdscatlen(o->ptr,…)` t_string.c:1429 — VAL realloc + **HDR.ptr write** on the kvobj. |
| SETRANGE (t_string.c:580-648) | P1, P3 (609), P12 (627), P11 | `kv->ptr = sdsgrowzero(...)` t_string.c:637, `memcpy(kv->ptr+offset,…)` t_string.c:640 — VAL + HDR.ptr. |
| INCR/DECR/INCRBY/DECRBY (`incrDecrCommand` t_string.c:901-940) | P1, P5 (`dbReplaceValueWithLink` 929), P3 (932), P11 | **`o->ptr = (void*)value` t_string.c:921** when refcount==1 && INT encoded: direct HDR.ptr write on the kvobj, no primitive. |
| INCRBYFLOAT (t_string.c:968-1030) | P1, P5 (985), P3 (987), P11 | — |
| INCREX (t_string.c:1213-1398) | P1, P6 (1334), P5 (1358), P3 (1360), P8 (1371), P7 (1374), P11 | **`o->ptr = (void*)value_ll` t_string.c:1350** same in-place INT write. |
| SETBIT (bitops.c:863-890) / BITFIELD (bitops.c:1977-2112) via `lookupStringForBitCommand` bitops.c:784-806 | P1 (790), P3 (795), P12 (799), P11 | `o->ptr = sdsgrowzero(o->ptr,byte+1)` bitops.c:803 and bit writes into `o->ptr` — VAL + HDR.ptr. |
| BITOP (bitops.c:1605-1609) | P5 via `setKey`, P6 | — |
| PFADD (hyperloglog.c:1645-1687) | P1, P3 (1655), P12 (1659), P11 | `hllSparseToDense` `o->ptr = dense` hyperloglog.c:673, `hllSparseSet` `o->ptr = sdsResize(...)` hyperloglog.c:716, register writes into `o->ptr` — VAL + HDR.ptr. |
| PFCOUNT (**read command**, hyperloglog.c:1742-1782) | P1 read, P12 (1749), P11 (1782) | Writes the cached cardinality `hdr->card[0..7]` into the VALUE (hyperloglog.c:1770-1777). |
| PFMERGE (hyperloglog.c:2021-2027) | P1, P12, P11 | dense register writes into VAL. |
| LCS (read) | P1 | none. STRLEN/GETRANGE/MGET read. |
| DELEX (db.c:1447-1531) | P1 read, P6 (1519-1521) | — |

### Hashes (t_hash.c)
| Command | Primitives | Bypassing in-place |
|---|---|---|
| HSET/HMSET (2147), HSETNX (2114) | P1/P3 via `hashTypeLookupWriteOrCreate` (1619-1629), `hashTypeTryConversion` (P14), P18, P11 | listpack/listpackEx VAL writes and `o->ptr =` (P18 internals t_hash.c:955, 972). |
| HINCRBY (2568) / HINCRBYFLOAT (2620) | P1, lazy field expiry via `hashTypeGetValue` (P19!), P3 (2595/2653), P18, P11 | as P18 |
| HDEL (3041-3110) | P1, P19 (3068), P6 `dbDeleteSkipKeysizesUpdate` (3090), P22 `estoreRemove` (3094), P11 | P19 VAL writes. |
| HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT (`hexpireGenericCommand` 4011-4137) | P1 (4019), P20 Init/SetEx/Done (4042/4057/4076), P6 (4093), P11 | LISTPACK→LISTPACK_EX conversion (HDR.encoding/ptr), TTL tuple rewrite in `lpt->lp`, `lpt->meta` via estore. |
| HPERSIST (4180-4324) | P1 (4184), `listpackExUpdateExpiry` (4271, VAL), `hfieldPersist` (4305, HT private ebuckets), P11 (4317) | Does **not** update global HFE (comment t_hash.c:3672-3679) — value-only. |
| HSETEX (2401-2566) | P1 (2414), P3 (2424), lazy expiry on read (2450), P20 (2478/2494/2504), P6 (2555), P11 | as HEXPIRE + P18 |
| HGETEX (2888-3040) | P1 (2895), P20 (2916/2942/2952), P6 (3036), P11 | as HEXPIRE |
| HGETDEL (2764-2886) | P1 (2770), P19 (2822), P22 (2845), P6 (2873), P11 | P19 |
| HGET/HMGET/HEXISTS/HSTRLEN/HLEN/HGETALL/HSCAN/HRANDFIELD (**reads**) | P1; `hashTypeGetValue` t_hash.c:713-815 lazily **deletes expired fields (P19 t_hash.c:782) and possibly the hash (`dbDelete` t_hash.c:806)**; HRANDFIELD calls `hashTypeExpireIfNeeded` → P21 (t_hash.c:3362, 3633) | VAL writes on a read. |

### Sets (t_set.c)
| Command | Primitives | Bypassing in-place |
|---|---|---|
| SADD (617-650) | P1, P3 (622), `setTypeAdd*` (P15 conversions), P11 | `set->ptr = intsetAdd(...)` t_set.c:116/175, `set->ptr = lp` 161/204 — VAL realloc + HDR.ptr. |
| SREM (652-697) | P1, `setTypeRemove` (`setobj->ptr = intsetRemove` 240/267, `lpDelete` 453), P6 (668), P11 | HDR.ptr |
| SPOP (834-1060) | P1, P6 (859), **P5 `dbReplaceValue`** (1028, SPOP count replaces the whole set), in-place `set->ptr = lp` (932/987), P11 | HDR.ptr |
| SMOVE (699-760) | P1×2, P6 (738), P3 (747), setTypeAdd/Remove, P11 | HDR.ptr |
| SRANDMEMBER (**read**, 1065) | P1 read; may `dbDelete` (1093) for an empty set | — |
| SINTERSTORE/SUNIONSTORE/SDIFFSTORE (1419/1538/1545, 1819/1827) | P6, P5 via `setKey`, P11; `dstset->ptr = lpShrinkToFit` (1536) on the *new* object before setKey | — |

### Sorted sets (t_zset.c, geo.c)
| Command | Primitives | Bypassing in-place |
|---|---|---|
| ZADD/ZINCRBY (2040-2110) | P1 (2040), P3 (2045), `zsetAdd` (in place `zobj->ptr = zzlDelete/zzlInsert` 1647-1661; P16 conversion), P11 | HDR.ptr for listpack; skiplist in place (SNAP_FORK). |
| ZREM (2112-2140) | P1, `zsetDel` (`zobj->ptr = zzlDelete` 1779), P6 (2126), P11 | HDR.ptr |
| ZREMRANGEBY* (2194-2260) | P1, `zobj->ptr = zzlDeleteRange*` (2220-2226), P6 (2232/2254), P11 | HDR.ptr |
| ZPOPMIN/MAX, BZPOP*, ZMPOP (4242-4356, 4427) | P1, in-place pops, P6 (4356), P11 | HDR.ptr |
| ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE (3020-3160) | P5 via `setKey` (3149), P6 (3158), P11 | — |
| ZRANGESTORE (3361-3367) | P5 via `setKey`, P6 | — |
| GEOADD → ZADD path; GEORADIUS STORE / GEOSEARCHSTORE (geo.c:702, 812-837) | P5 via `setKey`, P6 | — |

### Lists (t_list.c)
| Command | Primitives | Bypassing in-place |
|---|---|---|
| LPUSH/RPUSH/LPUSHX/RPUSHX (`pushGenericCommand` 490-540) | P1 (490), P3 (499), `listTypeTryConversionAppend` (P17), `listTypePush` (`subject->ptr = lpPrepend/lpAppend` 159-163), P11 | HDR.ptr (listpack), quicklist in place (SNAP_FORK) |
| LPOP/RPOP/LMPOP/BLMPOP (642-830) | P1, `listTypePop` (`subject->ptr = lpDelete` 197), P17 shrink, P6 (795), P11 | HDR.ptr |
| LSET (936-976) | P1, `listTypeReplaceAtIndex` (`o->ptr = lpReplace` 385), P17, P6 (974), P11 | HDR.ptr |
| LINSERT (`listTypeInsert` 343), LREM (1114-1150, iterator `lpDelete` 423), LTRIM (964-965 `o->ptr = lpDeleteRange`) | P1, P17, P6, P11 | HDR.ptr |
| LMOVE/RPOPLPUSH/BLMOVE (1210-1260) | P1×2 (1210, 1221), `listTypePop`, `lmoveHandlePush` → P3 (1168-1169) or push, `listElementsRemoved` → P6, P11 | HDR.ptr; holds `kvsrc`/`kvdst` raw pointers across pop→push (t_list.c:1230-1240). |
| Blocking pops served via `handleClientsBlockedOnKeys` (blocked.c:607 `lookupKeyReadWithFlags(LOOKUP_NOEFFECTS)`) then re-executing the command | same as the non-blocking command | no stashed kvobj pointers (blocked.c stores key names only). |

### Streams (t_stream.c)
| Command | Primitives | Bypassing in-place |
|---|---|---|
| XADD (2349-2358) | P1 (2349), P3 (2358), `streamAppendItem` (rax/listpack nodes in place, SNAP_FORK), IDMP register (`dictAddRaw(db->stream_idmp_keys)` 5971), P11 | VAL only (SNAP_FORK) — no HDR write (`o->ptr` is the `stream*`, never reassigned). |
| XDEL/XDELEX (3738), XTRIM (3860), XSETID (3677), XGROUP CREATE/SETID/DESTROY/CREATECONSUMER/DELCONSUMER (3480-3549; P3 at 3549 for MKSTREAM), XACK (4810), XAUTOCLAIM (4882), XCLAIM (5011), XACKDEL (5393), XREADGROUP (consumer/PEL updates on a read) | P1 write lookups, P11 | VAL in place (SNAP_FORK). `streamKeyRemoved` db.c:596/862 and `streamKeyLoaded` on RENAME/MOVE/COPY/RESTORE touch IDMP dict only. |
| Cron `handleExpiredIdmpEntries` t_stream.c:6010-6075 (server.c:1784) | `dbFind` (6029), in-place producer list/rax/dict edits (6055-6071), `dictDelete(db->stream_idmp_keys)` (6038) | VAL in place on a stream (SNAP_FORK) driven by cron, no `keyModified`. |

### Arrays / GCRA (t_array.c, gcra.c)
| Command | Primitives | Bypassing in-place |
|---|---|---|
| ARMSET/ARDEL/ARDELRANGE/ARINSERT/ARRING/ARSEEK/ARNEXT… (t_array.c) | `lookupArrayForWriteOrReply` t_array.c:49-59 (P1 + P3 at 53), P11 | `arSet(ar,…)` etc. mutate the `redisArray` in place (t_array.c:242); ARRING may reallocate: `o->ptr = ar = new_ar` t_array.c:1698 = **HDR.ptr write**. OBJ_ARRAY is not in `snapEncodingIsNoforkClass` → SNAP_FORK value; header write still matters. |
| GCRA (gcra.c:78-247), GCRA.SETVALUE (248-277) | P1 (133/254), **P5 via `setKeyByLink`** (202/266), P7 (214/271), P11 | OBJ_GCRA value is an INT-encoded ptr (object.c:518-533); `setKeyByLink` with ALREADY_EXIST → dbSetValue: old is INT (encoding != RAW but also != EMBSTR) and new is INT → **takes the header-swap path** (db.c:636) → HDR write on original. |

### Generic (db.c, expire.c, cluster.c, sort.c, object.c)
| Command | Primitives | Notes |
|---|---|---|
| DEL/UNLINK (`delGenericCommand` db.c:1415-1432) | P2 (1419), P6 (1420-1421), P11 | |
| EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT (expire.c:726-844) | P1 (752), P6 (807, past time), **P7** (820), P11 | P7 realloc path frees the fork-instant header and nulls its `ptr`. |
| PERSIST (expire.c:911-927) | P1, **P8** (914), P11 | META in place. |
| TTL/PTTL/EXPIRETIME/TYPE/EXISTS/TOUCH/OBJECT/DEBUG OBJECT (reads) | P1 (some with NOTOUCH) | may lazy-delete (P2). TOUCH explicitly bypasses CLIENT_NO_TOUCH (db.c:316) but not the child gate. |
| RENAME/RENAMENX (db.c:2189-2260) | P1×2, P6 dest (2220), P22 `estoreRemove` src (2229, value write), `keyMetaOnRename` (2234), **`incrRefCount(o)` 2206 (HDR.refcount on src)**, P6 src (2236, `decrRefCount` inside), **P3 with the same `o`** (2238) → P10 `kvobjSet(key, o, …)` steals `o->ptr` (**`o->ptr = NULL` object.c:303**) and frees the old header (object.c:328), P22 `estoreAdd` (2242), `streamKeyLoaded` (2246), P11 | The source key's fork-instant header is mutated (refcount, ptr) and freed. |
| MOVE (db.c:2270-2353) | P1 (2303), P1 on dst (2309), P22 (2322), `keyMetaOnMove` (2327), `incrRefCount(kv)` (2329), P6 (2330), P3 same object (2332), P22 (2337), P11 | same as RENAME |
| COPY (db.c:2355-2490) | P1 read (2409), P1 write dst (2415), type `*Dup` (fresh object), P6 (2448), `keyMetaOnCopy` (2453), P3 (2455), P22 (2460), P11 | source untouched except LRU (suppressed under child) |
| RESTORE / RESTORE-ASKING (cluster.c:~255-335) | `rdbLoadObject` (267), P6 (277, REPLACE), P3 (296) with KeyMetaSpec (ABSTTL/expire), P22 (306), `streamKeyLoaded` (310), **P29 `objectSetLRUOrLFU`** (321, HDR.lru on the NEW kvobj), P11 | fine (new header) except LRU write ordering after dbAdd. |
| DUMP | read | |
| MIGRATE | DUMP + DEL on success → P6 | |
| SORT … STORE (sort.c:620/629) | P5 via `setKey`, P6 | |
| OBJECT FREQ/IDLETIME/ENCODING/REFCOUNT (object.c:1704-1758) | `kvobjCommandLookup` = P1 with NOTOUCH|NONOTIFY (1693); `LFUDecrAndReturn` is read-only (evict.c:301-308) | may lazy-delete. |
| SCAN/KEYS/RANDOMKEY | `scanCallback` db.c:1703, `keysCommand`, `dbRandomKey` db.c:833 → P2 | lazy deletes on read |

### Keyspace-wide
| Command | Primitives |
|---|---|
| FLUSHALL / FLUSHDB (`flushCommandCommon` db.c:1336, `flushAllDataAndResetRDB` db.c:1223: `emptyData` then `killRDBChild()` db.c:1225) | P31 (sync or async→bio). `killRDBChild` already kills a stock RDB child on FLUSHALL, not on FLUSHDB. |
| SWAPDB (db.c:2646-2683) → `dbSwapDatabases` (2550) | swaps `keys/expires/subexpires/stream_idmp_keys` pointers between two `redisDb` (2568-2580). No child kill. |
| ASM slot migration: import (RESTORE-ASKING stream via fake master client cluster_asm.c:1439/2511, pre-`kvstoreDictExpand` 2215-2216), trim: background (`kvstoreMoveDict`/`estoreMoveEbuckets` cluster_asm.c:3058-3060 + `emptyDbDataAsync` 3065 → bio) or active (`asmActiveTrimDeleteKey` 3649-3665 → P6 per key, iterating `kvstoreInitDictSafeIterator` 3721) | P27/P32/P6 |

### Server-driven
| Path | Primitives | Existing gating |
|---|---|---|
| Lazy expiry on read | P2 from P1 (db.c:301), scan (1703), randomkey (833), KEYS | `PAUSE_ACTION_EXPIRE` (db.c:2993) |
| Active expire (keys) `activeExpireCycle` expire.c:287-546 → `kvstoreScan(db->expires,…, expireScanCallback)` expire.c:431 → `activeExpireCycleTryExpire` expire.c:40-54 → `deleteExpiredKeyAndPropagate` (47) → P6 | `kvstoreScan` uses `dictScan` (kvstore.c:359) — no rehash step, but the callback deletes from `db->expires` **while scanning it**. | `server.active_expire_enabled`, `isPausedActions(PAUSE_ACTION_EXPIRE)` (checked in expire.c ~300) |
| Active expire (hash fields) `activeSubexpiresCycle` expire.c:228-285 → `activeSubexpires` (208) → `estoreActiveExpire` (estore.c:115) → `ebExpire` (**sets `trash=1` on the hash's ExpireMeta inside the value** ebuckets.c:377/691) → `activeSubexpiresCb` expire.c:170-195 → P21 → `ebSetMetaExpTime` (191, value write) → re-`ebAdd` | | same |
| Maxmemory eviction `performEvictions` evict.c:532 (`evictionPoolPopulate` evict.c:134 reads `lru`/expire only) → `deleteEvictedKeyAndPropagate` db.c:2831 → P6 | | none for children |
| Active defrag P30 | | `hasActiveChildProcess()` defrag.c:1834/1981 |
| Cron dict resize/rehash P28 | | `!hasActiveChildProcess()` server.c:1326 + `DICT_RESIZE_AVOID` server.c:824 |
| `expireSlaveKeys` expire.c:548 (writable replica) → P2-like delete | | |
| IDMP cron `handleExpiredIdmpEntries` (see streams) | | none |
| DEBUG RELOAD (debug.c:607-653): `rdbSave` (633, foreground save — runs `rdbSaveDb` on main thread incl. `updateSlotAllocSize` writes rdb.c:1839-1841), `emptyData` (642, P31), `rdbLoad` (645 → P4, P22, `streamKeyLoaded`, P29 at rdb.c:4381-4404, plus `kvstoreDictExpand` rdb.c:4171-4172) | | `DEBUG RELOAD` calls `rdbSave` which fails/blocks if a child exists? (rdbSave itself does not check; `bgsaveCommand` does) |
| DEBUG LOADAOF (654-670): `emptyData` + `loadAppendOnlyFiles` (commands → normal primitives) | | |
| DEBUG POPULATE (788-830): P1 + P3 (826) | | |
| DEBUG DIGEST-VALUE / OBJECT / SDSLEN / LISTPACK / QUICKLIST / HTSTATS-KEY: `dbFind` (no touch), read-only | | |
| DEBUG DICT-RESIZING (1139) flips `dict_can_resize` → can re-enable rehash on find while a save is running | | |
| DEBUG SET-ACTIVE-EXPIRE / SET-ALLOW-ACCESS-EXPIRED / CHANGE-REPL-ID: no keyspace mutation | | |
| Replication applying writes: same command procs with `CLIENT_MASTER` (expiry suppressed db.c:2971-2972). Full sync load: `emptyData` (replication.c:2503/2554) or `disklessLoadInitTempDb`+`swapMainDbWithTempDb` (2508/2582); **kills an RDB child first** (replication.c:2451 `killRDBChild`). |  | |
| Module writes | see §2 "Modules" | GIL: `RM_ThreadSafeContextLock` module.c:9144 → `moduleAcquireGIL` 9187; module threads mutate only while holding the GIL (main thread blocked). |
| Lua/Functions/MULTI-EXEC | reduce to command procs (eval.c/script_lua.c/functions.c/multi.c contain no db primitives; only `ldbAddBreakpoint` matched). | |

### Modules (module.c)
| API | Primitives | In-place |
|---|---|---|
| `RM_OpenKey` 4168 (WRITE → `lookupKeyWriteWithFlags` 4181) / `RM_CloseKey` → `moduleCloseKey` 4214 (P11 at 4217) | P1, P11 | |
| `RM_DeleteKey` 4279 / `RM_UnlinkKey` 4293 / `moduleDelKeyIfEmpty` 774 | P6 | |
| `RM_SetExpire` 4322 / `RM_SetAbsExpire` 4354 | P7 (4328/4360), P8 (4330/4362) | |
| `RM_StringSet` 4681 | P6 then P5 via `setKey` (4686) | |
| `RM_StringDMA` 4720 | P12 (4746) | returns `key->kv->ptr` (4749) for the module to write **arbitrarily later**, outside any primitive. |
| `RM_StringTruncate` 4754 | P12 (4776), P5 via `setKey` (4771) | `key->kv->ptr = sdsgrowzero` (4782), `sdssubstr` (4784), `sdsRemoveFreeSpace` (4787) — VAL + HDR.ptr. |
| `RM_ModuleTypeSetValue` 7560 | P6 + P5 via `setKey` (7564) | |
| `RM_ModuleTypeReplaceValue` 14898 | none | `mv->value = new_value` (14910) — VAL (module value struct, SNAP_FORK). |
| `RM_ListPush/Pop/Set/Insert/Delete` 4878-5140, `RM_ZsetAdd/Incrby/Rem` 5189-5300, `RM_HashSet` 5692, `RM_StreamAdd/Delete/Trim` 5974-6100 | type-layer helpers (same as commands: P17/P16/P18/P19 conversions, `o->ptr` writes) + `moduleDelKeyIfEmpty` (P6) | HDR.ptr for listpack encodings. |
| `RM_SetLRU` 14762 / `RM_SetLFU` 14789 | P29 | HDR.lru in place, **not gated by child**. |
| `RM_SetKeyMeta` 4605 | P23 | META in place or header realloc. |
| Module `unlink`/`free` keymeta callbacks (P25) | | may write META slot via `pMeta` pointer; `free` may run on bio thread. |
| `RM_ResetDataset` 4634 | P31 | |
| Module `defrag` callbacks | P30 (gated) | |

---

## 3. Reads that write

| # | Site | What it writes | Gate |
|---|---|---|---|
| R1 | `lookupKey` db.c:314-324: `updateLFU(val)` (db.c:55-59 `val->lru = …`) or `val->lru = LRU_CLOCK()` (db.c:322) | HDR.lru on every hit (LRU/LFU policies) | `!hasActiveChildProcess()` (db.c:319), `LOOKUP_NOTOUCH`, `CLIENT_NO_TOUCH` (db.c:314-317, TOUCH excepted). Not written under LRM policy on reads (db.c:320-323). |
| R2 | `keyModified` → `updateLRM` db.c:62-68 | HDR.lru on every *modification* under `MAXMEMORY_FLAG_LRM` | **none** (not child-gated) |
| R3 | `expireIfNeeded` deletion from read lookups (db.c:2996-3004), `scanCallback` db.c:1703, `dbRandomKey` db.c:833, KEYS | P6 (full delete + propagate) | `PAUSE_ACTION_EXPIRE` db.c:2993; replicas |
| R4 | `hashTypeGetValue` t_hash.c:713-815: `hashTypeDelete` (782, VAL), `propagateHashFieldDeletion` (784), `dbDelete` of empty hash (806), `keyModified` (812) — reached from HGET/HMGET/HEXISTS/HSTRLEN/HINCRBY*/HSETEX/HGETEX/`RM_HashGet` (`addHashFieldToReply` t_hash.c:2689-2699) | VAL (listpackEx `lpt->lp` realloc) / P6 | `HFE_LAZY_AVOID_FIELD_DEL`, `PAUSE_ACTION_EXPIRE`, replica (t_hash.c:757-775) |
| R5 | `hashTypeExpireIfNeeded` t_hash.c:1988-2016 from HRANDFIELD (3362, 3633) → P21 (VAL, HFE, possibly P6) | | same conditions (t_hash.c:1998-2003) |
| R6 | PFCOUNT hyperloglog.c:1749-1782: `dbUnshareStringValue` (may P5!) + `hdr->card[]` writes + `keyModified` | VAL, possibly whole header replacement | none |
| R7 | `LFUDecrAndReturn` evict.c:301-308 (OBJECT FREQ, eviction sampling) | **read-only** (computes, does not store) — NOOP |  |
| R8 | `OBJECT IDLETIME/FREQ/ENCODING/REFCOUNT` object.c:1704-1758 via `kvobjCommandLookup` (NOTOUCH) | only R3 | |
| R9 | `evictionPoolPopulate` evict.c:134-200 | reads `lru`, `kvobjGetExpire` only — NOOP; but `performEvictions` deletes (P6) | |
| R10 | `handleClientsBlockedOnKeys` blocked.c:607 `lookupKeyReadWithFlags(LOOKUP_NOEFFECTS)` | NOOP (no touch, no expire) | |
| R11 | Keysizes / alloc-size histograms: `updateKeysizesHist` db.c:121, `updateSlotAllocSize` db.c:126-147 write kvstore metadata (`kvstoreGetMetadata`, `kvstoreGetDictMeta`) — also from `hashTypeGetValue` on HT reads (t_hash.c:741-746) and from `rdbSaveDb` (rdb.c:1839-1841) | kvstore metadata (in the SNAP_NOFORK-arena kvstore struct per spec §1), not kvobj/value. Bg thread does not read them mid-scan (only sizes at START). NOOP for the invariant, **but the bg thread must not call `updateSlotAllocSize` itself** if it reuses `rdbSaveDb`. |
| R12 | Hotkeys (`hotkeyStatsUpdateCurrentCmd` hotkeys.c:107-170) | only `argv[pos]->ptr` sds and its own top-k sketch — NOOP |  |
| R13 | XREADGROUP / XCLAIM / XAUTOCLAIM update consumer PEL/last-seen (stream VAL, SNAP_FORK) on "read-ish" commands | FORK_VALUE_IN_PLACE |  |
| R14 | Quicklist reads decompress/recompress nodes in place (`quicklistDecompressNodeForUse` quicklist.c:309, `quicklistRecompressOnly` quicklist.c:423) e.g. LINDEX/LRANGE | VAL (SNAP_FORK quicklist) — child has CoW copy; but the LRU-suppressed read path still *writes* SNAP_FORK memory: FORK_VALUE_IN_PLACE, no header change. |  |
| R15 | Memory prefetch (`dictPrefetcherRun` memory_prefetch.c:263-280) walks `db->keys` buckets → entries → kvobj key → `kv->ptr`, issuing prefetch hints only | read-only, but runs on **IO threads** (iothread.c:508) as well as main (networking.c:3749). |  |

---

## 4. Hash tables and expiry structures

### `db->keys` / `db->expires` (kvstore of dicts)
| Op | Sites |
|---|---|
| Insert into keys | P3 `kvstoreDictSetAtLink(newItem=1)` db.c:436; P4 db.c:533; (dict `dictSetKeyAtLink` → `dictExpandIfNeeded` dict.c:891 may allocate/expand the table). |
| Rewrite existing entry (pointer swap) | P5 db.c:658; P7 db.c:2738; P23 keymeta.c:842; defrag defrag.c:1130/1544; async delete sets NULL db.c:889. |
| Delete from keys | P6 db.c:848/891 (`dictTwoPhaseUnlinkFind` pauses rehash between the two phases dict.c:958/978). |
| Insert into expires | P3 via P7 db.c:2745 `kvstoreDictAddRaw` (→ `dictAddRaw` dict.c:526: `_dictRehashStep` + `_dictExpandIfNeeded`). |
| Rewrite expires entry | P5 db.c:667; P7 (none — new kv added); P23 keymeta.c:852; defrag defrag.c:1132/1546. |
| Delete from expires | P5 db.c:669 (`kvstoreDictDelete`), P6 db.c:882, P8 db.c:2694 (two-phase). |
| Rehash step on lookup | every `dictFindLink`/`dictFind` (dict.c:800-862 → `dictFindLinkInternal` → `_dictRehashStepIfNeeded` dict.c:1729-1742) — includes `dbFind`, `dbFindByLink`, `lookupKey`, `kvstoreDictFindLink`, `kvstoreDictFind` (defrag later-step defrag.c:1352, eviction pool evict.c:~631, IDMP cron `dbFind` t_stream.c:6029). Suppressed by `pauserehash` (dict.c:1730) — which is what `snapScanFreezeStore` snapshot.c:347 sets. |
| Cron resize/rehash | server.c:1340-1353 (P28) — gated by `!hasActiveChildProcess()`; policy `updateDictResizePolicy` server.c:821-828. `dictPauseAutoResize` (used by snapshot.c:352) blocks `dictExpandIfNeeded`/`dictShrinkIfNeeded`. |
| Pre-expand | RDB load rdb.c:4171-4172; ASM import cluster_asm.c:2215-2216; `dbExpand` db.c:3039-3045. |
| Whole-dict moves | `kvstoreMoveDict` kvstore.c:557 (ASM bg trim cluster_asm.c:3058-3059) |
| Whole-kvstore replace/free | `emptyDbStructure` db.c:973 (`kvstoreEmpty`), `emptyDbAsync` lazyfree.c:325-343 (new kvstores + bio free of the old), `dbSwapDatabases` db.c:2568-2580, `swapMainDbWithTempDb` db.c:2616-2628, `RM_ResetDataset`. |
| Iteration that deletes while scanning | active expire `kvstoreScan(db->expires)` expire.c:431 with deleting callback; `asmActiveTrimCycle` safe iterator over `db->keys` slot deleting (cluster_asm.c:3721-3728); SCAN (`scanGenericCommand` db.c:1813 → `kvstoreScan`) with lazy-expire deletes. |
| Defrag LUT | `kvstoreDictLUTDefrag` kvstore.c:800 (reallocates the `dict` struct itself) — defrag stage, gated. |

### HFE: `db->subexpires` (estore of ebuckets) and ExpireMeta in values
| Op | Sites | Writes value memory? |
|---|---|---|
| `estoreAdd` | P3-adjacent: RENAME db.c:2242, MOVE db.c:2337, COPY db.c:2460, RESTORE cluster.c:306, rdb load rdb.c:4396; P7 db.c:2749; P23 keymeta.c:857; P14 t_hash.c:1742; P20 done t_hash.c:1282; P21 t_hash.c:1959 | **yes** — `ebAdd` writes `lpt->meta`/dict-meta ExpireMeta (`next`, flags, expire time) |
| `estoreRemove` | P5 db.c:593; P6 db.c:858; P7 db.c:2733; RENAME db.c:2229; MOVE db.c:2322; P23 keymeta.c:820; P14 t_hash.c:1702; P20 done t_hash.c:1279; P21 t_hash.c:1949; HDEL t_hash.c:3094; HGETDEL t_hash.c:2845 | **yes** — `ebRemove` marks `trash=1` and relinks |
| `estoreUpdate` | P20 done t_hash.c:1277 | yes |
| `estoreActiveExpire` → `ebExpire` | expire.c:221 | yes: `trash=1` (ebuckets.c:377/691) on each visited hash before the callback, `ebSetMetaExpTime` (expire.c:191), re-add |
| `estoreMoveEbuckets` | ASM bg trim cluster_asm.c:3060 | no (moves bucket ownership) |
| `estoreEmpty` | `emptyDbStructure` db.c:990 | frees buckets; items not written |
| Defrag | `defragStageSubexpires` defrag.c:1552-1599 → `ebScanDefrag` relocates items via `activeDefragSubexpiresOB` (1522) and rewrites links | gated |
| Per-hash private HFE (HT encoding only, `htMetadataEx->hfe`) | `hfieldPersist` t_hash.c:3660, `hashTypeSetExpiryHT` t_hash.c:1067, `hashTypeSet` HT branch t_hash.c:1040-1058, `hashTypeConvertListpackEx` t_hash.c:1731 | dict metadata = SNAP_FORK value (HT) |
| ListpackEx TTL tuple edits (`lpt->lp` realloc) | `listpackExAddInternal` t_hash.c:438-460 (441/456/458), `listpackExAddNew` 464, `listpackExUpdateExpiry` 478 (502), `listpackExExpire` 426, P18 (972), P19 (1314), HPERSIST 4271 | yes (SNAP_NOFORK value realloc) |
| `initDictExpireMetadata` t_hash.c:1194 / `hashTypeSetExInit` dictTypeAddMeta t_hash.c:1221-1224 | HT only; `o->ptr = ht` HDR write | |

### Stream IDMP dict `db->stream_idmp_keys`
Registered/unregistered at t_stream.c:5971/5983/5994, db.c:596/862 (P5/P6), moved by `streamMoveIdmpKeys` db.c:1113; not part of the snapshot content — NOOP for the RDB, but it holds key **names**, not kvobj pointers.

---

## 5. Non-main-thread mutators/readers

| Actor | What it touches | Sites |
|---|---|---|
| **bio lazy-free thread** | `decrRefCount` on values (`lazyfreeFreeObject` lazyfree.c:14-19) — the object handed over is the *kvobj* (`freeObjAsync(key, kv, …)` db.c:887 with `kv` = the kvobj; db.c:707 passes the *old value* which, on the swap path, is the caller's `val` header). When a kvobj is freed on bio, `decrRefCount` runs `keyMetaOnFree` (object.c:668, module callbacks on bio) and `zfree(alloc)` of the **header** (object.c:689) — under the spec this is a SNAP_NOFORK-arena free that must go through `snapNoforkFree`, and must not happen while the slot is owed. Also `kvsLazyfreeFree` lazyfree.c:55-81 frees whole kvstores/estores (FLUSH ASYNC, ASM bg trim). Only frees; the freed memory may be **re-used by the allocator** while the bg thread still reads it (use-after-free, not just a torn read). |
| **IO threads** | `freeDeferredObject` networking.c:1713-1721 `decrRefCount` of RAW string values replaced by P5 (db.c:705) — value only (header stays); memory prefetch of `db->keys` buckets/entries/kvobj/`kv->ptr` (memory_prefetch.c:263-280 from iothread.c:508) — read-only hints; IO threads never take kvobj pointers otherwise (replies copy). |
| **Module threads** | Any key API only under the GIL (`moduleAcquireGIL` module.c:9187), i.e. while the main thread is parked — logically "main thread" for the invariant, but hook placement must not rely on `server.current_client`/`executing_client`. Timers/notifications run on main. |
| **RDB child (fork)** | Read-only over `rdbSaveRio` rdb.c:1880 → `rdbSaveInfoAuxFields`, `rdbSaveFunctions`, `rdbSaveModulesAux`, per-db `rdbSaveDb` rdb.c:~1780-1870 (iterates `db->keys` via `kvstoreIteratorNext` 1806, reads `kvobjGetKey`/`kvobjGetExpire` 1835-1836, `rdbSaveKeyValuePair` 1839 → `rdbSaveKeyMetadata` rdb.c:1614 reads META slots, `rdbSaveObject` reads values incl. **`lpt->meta` for LISTPACK_EX** rdb.c:1241), `dismissObject` (child only, rdb.c:1851). In the hybrid design the child must read none of the above except SNAP_FORK values; kvobj copies arrive via the channel. |
| **AOF rewrite child** | `rewriteAppendOnlyFileRio` aof.c:2676 (own key loop, `dictGetKV` 2719) or `rdbSaveRio` with preamble (aof.c:2805). Same footprint. |
| **Defrag** | main thread but relocates kvobj allocations, dict tables, listpackEx structs, ebuckets links (P30); gated off while a child exists (defrag.c:1834, 1981). |
| **snap scan thread** (new) | `dictScan` over frozen dicts (snapshot.c:370-375 → dispatch with `dictGetKV(de)`); must stay read-only; must **not** call `updateSlotAllocSize`, `kvobjAllocSize`-with-side-effects, `hashTypeGetMinExpire(o,0)` on a concurrently-updated `lpt->meta`, etc. |

---

## 6. Treatment proposal per entry

Hook predicate: `snapScanKeyIsOwed(ctx, dbid, slot)` (snapshot.c:508) evaluated once per primitive call with the slot from `getKeySlot(key)`. "Temp dict" = per-slot main-thread dict keyed by sds key → {clone kvobj | tombstone}.

### Header-only mutations → CLONE_HEADER
| Entry | Hook |
|---|---|
| EXPIRE/SET EX/GETEX/MSETEX/INCREX/GCRA/`RM_SetExpire` via P7 `setExpireByLink`, both branches (in-place META write db.c:2723 and realloc db.c:2735) | In P7: if owed and key not yet in temp dict → `snapCloneKvobj(kv)` (allocate new kvobj with `KEY_META_MASK_EXPIRE` set, copy META/robj/key, **share `ptr`**, do not touch original), write expire into the clone, insert clone into temp dict; skip `kvstoreDictSetAtLink`/`kvstoreDictAddRaw` on the live dicts (record "has expire" in the temp entry so merge adds the EXP entry). `kvobjSetExpire`'s `kvobjSet(key, kv, …)` path must be bypassed because it does `val->ptr = NULL` + `decrRefCount(val)` (object.c:303/328) on the original. |
| PERSIST / GETEX PERSIST / INCREX PERSIST / `RM_SetExpire(NO_EXPIRE)` via P8 `removeExpire` (db.c:2692 META write, 2694 EXP delete) | Same in P8: clone with expire=-1 into temp dict; defer the EXP unlink to merge. |
| `keyModified`→`updateLRM` (db.c:66) under LRM policy | In `updateLRM`: if the object is a kvobj (`o->iskvobj`) and its key is owed → write `lru` on the temp-dict clone instead (the caller already has/creates one for any write; for pure-LRM-touch with no other change, either CLONE_HEADER or NOOP by skipping the LRM update while owed — LRM is an approximation; recommend **NOOP (skip when owed)**, mirroring the existing `hasActiveChildProcess()` skip in `lookupKey`). |
| `RM_SetLRU`/`RM_SetLFU` (P29 module.c:14765/14792), RESTORE IDLETIME/FREQ (cluster.c:321) | RESTORE: the kvobj is brand new (ADD_TEMP) — write goes to the temp clone, fine. Module: CLONE_HEADER inside `objectSetLRUOrLFU` when `val->iskvobj` and owed. |
| `RM_SetKeyMeta` → P23 (in-place keymeta.c:802 or realloc 838) | CLONE_HEADER in P23 (clone gets the new metabit; original untouched; skip KEYS/EXP rewrite until merge). `estoreRemove/Add` there → see HFE below. |
| `incrRefCount`/`decrRefCount` on the fork-instant kvobj header (RENAME db.c:2206, MOVE db.c:2329, P5 db.c:614/627, P6 db.c:866/875) | These exist only to keep the header alive across module unlink callbacks that might re-enter (comments db.c:611-613, 864-865). Treat as **ATOMIC-tolerated**: the RMW writes back identical `type/encoding/lru/metabits` bits; the bg thread only reads those and `ptr`. Formally a data race on the header word; acceptable if the copier reads the header word once with a relaxed atomic load and validates `type/encoding` are sane. Better: in the owed case P5/P6 operate on the temp clone (refcount juggling happens on the clone), so the original is never touched. |
| `dbSetValue` header-swap path (db.c:636-652) — SET over RAW string, GCRA, SPOP-count `dbReplaceValue`, INCRBYFLOAT/INCR `dbReplaceValueWithLink`, `dbUnshareStringValue` decode | **CLONE_HEADER_VALUE**: when owed, never swap into `old`; allocate the clone (new kvobj via `kvobjSet(key, val, bits)` — `val` is the *new* value, so its `ptr` is stolen legitimately), put it in the temp dict; skip `removeExpire`/EXP rewrite; defer `keyMetaTransition` (keymeta.c:708 writes OLD META) by copying META into the clone without resetting the original (module `unlink` callbacks still fire once, at clone time, which matches today's "overwrite = unlink+add" semantics db.c:621-626). |
| Encoding conversions P14/P15/P16/P17 (`o->encoding =`, `o->ptr =`) and every `o->ptr = lp…/intset…` realloc in t_list/t_set/t_zset/t_hash/t_string/bitops/hyperloglog/t_array (§2 tables) | These are **value** mutations that surface as HDR.ptr writes. The only viable hook is upstream: **`lookupKeyWrite*` returns the temp-dict clone** when the key is owed (creating it on first write-lookup — CLONE_HEADER, sharing `ptr`), and additionally for SNAP_NOFORK classes **clones the value eagerly** (CLONE_HEADER_VALUE: `lpDup`/`intsetDup`/`sdsdup`/listpackEx copy) so all subsequent `o->ptr =` / `lpReplace` / `sdscatlen` writes land in the clone. For SNAP_FORK values (quicklist/HT/skiplist/stream/module/array/GCRA-int) the clone shares `ptr` and the in-place value edits are FORK_VALUE_IN_PLACE. Class change (listpack → HT, intset → HT, listpack → quicklist, LISTPACK_EX → HT) then happens on the clone: the new SNAP_FORK value is allocated in the default arena and the clone's SNAP_NOFORK duplicate is freed (CoW-free); the fork-instant original stays intact for the bg thread. |
| INCR in-place INT (`o->ptr = (void*)value` t_string.c:921, 1350), APPEND/SETRANGE/SETBIT/PFADD/PFCOUNT/`RM_StringTruncate` sds writes | Covered by the same `lookupKeyWrite`-returns-clone rule (INT: CLONE_HEADER, the "value" is the ptr word itself; RAW sds: CLONE_HEADER_VALUE with `sdsdup`; EMBSTR: `dbUnshareStringValue` already replaces via P5 → clone path). |
| `hashTypeGetValue`/`hashTypeExists` lazy field deletion on READ (t_hash.c:782, 806) and HRANDFIELD `hashTypeExpireIfNeeded` | These are reached through **`lookupKeyRead`**. Options: (a) SUSPEND_DURING_SAVE for owed keys — return `GETF_EXPIRED` without deleting (there is already a flag path: `HFE_LAZY_AVOID_FIELD_DEL` t_hash.c:766, and the replica branch returns EXPIRED without deleting t_hash.c:758-763); implement as "if owed → behave as `HFE_LAZY_AVOID_FIELD_DEL`". Correctness is preserved (expired fields are invisible either way and active expire catches up after merge). Recommended. (b) CLONE_HEADER_VALUE via a write-lookup upgrade. (a) is simpler and mirrors the master/replica asymmetry that already exists. Same for HRANDFIELD (`hashTypeExpireIfNeeded` → return 0 when owed). |
| PFCOUNT cache write (hyperloglog.c:1749-1777) | SUSPEND_DURING_SAVE: skip `dbUnshareStringValue` + cache write when owed (compute and reply without caching; `HLL_VALID_CACHE` is an optimisation). Or route through the write-lookup clone. |
| Lazy key expiry on read (P2 from P1/scan/randomkey/KEYS) | **TOMBSTONE**: `expireIfNeeded` → `deleteExpiredKeyAndPropagate` → P6; P6 in owed mode inserts a tombstone and leaves the entry. Reads already consult the temp dict first (spec §4), so the key becomes invisible immediately and propagation stays as today. |
| Active expire (`activeExpireCycleTryExpire` → P6) | TOMBSTONE via P6 hook. Note the callback runs inside `kvstoreScan(db->expires)`; since the live EXP dict is not modified while owed, the scan cursor stays valid. Bound the tombstone count with the spec's watermark. Alternative SUSPEND (skip expiring keys in owed slots) is acceptable for M1 since expiry is best-effort; but P6 must still be hooked for the command-driven deletes. |
| Eviction (`performEvictions` → P6) | TOMBSTONE via P6. Memory is *not* freed until merge (originals kept) — eviction under a save would then not reduce RSS for owed slots. Recommend SUSPEND_DURING_SAVE for candidates in owed slots plus the spec's hard watermark → ABORT_SAVE; otherwise eviction can loop forever ("freed" bytes are 0). Must be decided explicitly. |
| DEL/UNLINK/GETDEL/DELEX/`RM_DeleteKey`/`moduleDelKeyIfEmpty`/type commands deleting the emptied collection (`dbDeleteSkipKeysizesUpdate` t_list.c:974, t_set.c:668, t_zset.c:2126…) / RENAME dest & src / MOVE src / COPY REPLACE dest / RESTORE REPLACE / ASM active trim (`asmActiveTrimDeleteKey`) | **TOMBSTONE** in P6 when owed: run the module unlink / keyMeta unlink / signal / IDMP / HFE bookkeeping as today **except**: no `estoreRemove` on a LISTPACK_EX original (it writes the value — record "removed from HFE" in the tombstone and do it at merge), no `kvstoreDictDelete` on EXP, no two-phase unlink on KEYS, no free (defer to merge). `incrRefCount/decrRefCount` on the original → skip (operate on nothing; the header is immutable). |
| Adds: P3 `dbAddInternal`, P4 `dbAddRDBLoad` (RESTORE, DEBUG POPULATE, RENAME/MOVE re-add, COPY, MKSTREAM, every create-on-write) | **ADD_TEMP** in P3: build the kvobj exactly as now (it is new memory) but insert into the temp dict, skip `kvstoreDictSetAtLink` on KEYS and EXP, and for hashes skip `estoreAdd` (record min-expire in the temp entry). RENAME/MOVE pass the *existing* `o` to P3 → P10 `kvobjSet(key, o, …)` would null/free the original header (object.c:303/328): when `o` is a fork-instant kvobj (owed slot **of the source**), P3 must instead deep-copy the header and share `ptr` (CLONE_HEADER) and leave the source to its tombstone. The refcount dance (db.c:2206/2329) must be skipped for owed sources. |
| HFE registration writes into `lpt->meta` (P22 from P5/P6/P7/P14/P20/P21/P23/RENAME/MOVE/COPY/RESTORE, `ebSetMetaExpTime` expire.c:191, `ebExpire` `trash=1`) | **CLONE_HEADER_VALUE for every LISTPACK_EX hash whose HFE registration changes while owed**: the clone gets a fresh `listpackEx` (its own `ExpireMeta`) and is what gets `estoreAdd`ed; the original is `estoreRemove`d **at merge**, not now. Until then the original stays registered in `db->subexpires` pointing at the fork-instant struct — so active subexpire (`ebExpire`) may still *visit* it and write `trash=1` (ebuckets.c:377): the active-subexpire callback must therefore **skip (ACT_STOP or leave untouched) items whose key is owed** — SUSPEND_DURING_SAVE for `activeSubexpiresCb` on owed slots (check `snapScanKeyIsOwed` on `getKeySlot(kvobjGetKey(kv))` before P21). Because `ebExpire` sets `trash=1` *before* invoking the callback (ebuckets.c:377/691) and only restores it on `ACT_STOP_ACTIVE_EXP` (385/699), the skip must return ACT_STOP (which ends the bucket's cycle) — acceptable for M1; or add a new ExpireAction "skip without touching" to ebuckets. For HT-encoded hashes the ExpireMeta lives in dict metadata (SNAP_FORK) → FORK_VALUE_IN_PLACE is fine, but the *bg serializer* reads `hashTypeGetMinExpire(o,1)` (rdb.c:1262) which walks the private ebuckets — HT hashes are SNAP_FORK, serialized by the child from CoW, so OK. |
| `hashTypeSetExInit` first-time TTL on a LISTPACK hash (LISTPACK→LISTPACK_EX conversion P14 t_hash.c:1215, HDR.encoding/ptr) | CLONE_HEADER_VALUE via write-lookup clone (see conversions). |
| `RM_StringDMA` writable pointer | The clone rule in `lookupKeyWrite` (RM_OpenKey WRITE → module.c:4181) plus `dbUnshareStringValue` makes `key->kv` the clone, so the returned `key->kv->ptr` (module.c:4749) is clone memory. Must also **pin** the slot: if the slot completes and merges while the module still holds the DMA pointer, the merge must not free the clone (it does not — merge frees *originals*). OK given the module rule "no other key writing function while using DMA". |
| `RM_ModuleTypeReplaceValue` (`mv->value =` module.c:14910) | FORK_VALUE_IN_PLACE (moduleValue struct is SNAP_FORK memory); no header change; the key must still be recorded in the temp dict as "modified" only if the design wants dirty tracking — otherwise NOOP for the snapshot (child serializes the fork-instant `mv->value`). |
| Stream mutations (XADD…, IDMP cron, consumer-group updates on XREADGROUP) and other SNAP_FORK in-place edits (quicklist, HT set/hash/zset skiplist, redisArray, module values, quicklist decompress-on-read) | **FORK_VALUE_IN_PLACE**; header untouched (`o->ptr` unchanged). ARRING's `o->ptr = new_ar` (t_array.c:1698) and any quicklist/`zset*` `o->ptr` re-assignments do need the header clone — covered by the write-lookup clone rule. |
| Keysizes / alloc-size histograms, hits/misses, `server.dirty`, hotkeys | NOOP (kvstore metadata / globals; the bg thread must not write them). |
| `lookupKey` LRU/LFU on read | NOOP today thanks to `hasActiveChildProcess()` (db.c:319) — keep the fork so the gate holds; add an explicit `snapSaveInProgress()` to the condition so a future fork-less mode stays safe. |
| `OBJECT FREQ` / eviction sampling `LFUDecrAndReturn` | NOOP (read-only). |
| FLUSHALL/FLUSHDB/SWAPDB/`RM_ResetDataset`/DEBUG RELOAD/DEBUG LOADAOF/replica full-sync load/`swapMainDbWithTempDb`/ASM background trim (`kvstoreMoveDict` + `emptyDbDataAsync`) | **ABORT_SAVE** before mutating (spec §7.5). `flushAllDataAndResetRDB` already kills the RDB child (db.c:1225) but only for FLUSHALL; replication already `killRDBChild()` (replication.c:2451). Add `snapScanAbort` + join in `emptyData`, `dbSwapDatabases`, `swapMainDbWithTempDb`, `asmTriggerBackgroundTrim`, `kvstoreMoveDict` callers. |
| Cron dict resize/rehash (P28), rehash-step-on-find, `dictExpandIfNeeded` on insert | NOOP by construction: `hasActiveChildProcess()` gate (server.c:1326) + `snapScanFreezeStore` (`dictPauseRehashing`/`dictPauseAutoResize`) on every dict of an owed store; adds go to the temp dict so no insert-time expand. **Risk**: `DEBUG DICT-RESIZING` (debug.c:1139) and `dict_can_resize` only affect policy, not `pauserehash`, so the freeze is what matters — make sure PENDING slots are frozen too, not only SCANNING (the spec pauses only during SCANNING; but `lookupKey` on a PENDING slot performs `_dictRehashStepIfNeeded` which moves buckets — harmless for correctness of a later `dictScan`, but it *frees* the old bucket array when rehash completes, and the spec puts bucket arrays in the SNAP_NOFORK arena; fine as long as the bg thread has not started that slot). |
| Active defrag P30 | NOOP (gated by `hasActiveChildProcess`); add the explicit `snapSaveInProgress()` check next to defrag.c:1834/1981 for robustness. |
| bio/IO-thread frees of replaced values (db.c:705/707, 887) | When owed: frees are deferred to merge (spec M1 defers all frees to save-end) — so **do not** call `freeObjAsync`/`tryDeferFreeClientObject` on originals; only on clones being replaced by later clones (safe). |
| `keyMetaOnFree` on bio thread for a freed kvobj | Only happens at merge/save-end under the deferral, so no overlap with the bg read. |

---

## 7. Blockers / risks

1. **`lpt->meta` (HFE ExpireMeta) is inside the SNAP_NOFORK value and is both written by the main thread and read by the serializer.** Writers: `estoreAdd/Remove/Update` (P22) from P5/P6/P7/P14/P20/P21/P23/RENAME/MOVE/COPY/RESTORE, `ebExpire` (`trash=1`, ebuckets.c:377/691), `ebSetMetaExpTime` (expire.c:191), defrag link rewrite. Reader: `rdbSaveObject` rdb.c:1240-1241 → `hashTypeGetMinExpire(o,0)` t_hash.c:2026-2042 (`trash`, packed time). Not covered by any db.c primitive hook alone; needs (a) LISTPACK_EX clones to get their own `listpackEx`, (b) deferral of `estoreRemove(original)` to merge, (c) active-subexpire skip for owed keys, and (d) the bg serializer should compute the min expire from the listpack tuples (`listpackExGetMinExpire` t_hash.c:355, accurate mode) instead of reading `meta` — the RDB format stores a hint, and `rdbLoad` re-registers from the accurate value anyway (rdb.c:4394-4396).
2. **`kvobjSet(key, kv_existing, bits)` nulls and frees the fork-instant header** (object.c:303 `val->ptr = NULL`, object.c:328 `decrRefCount(val)`), used by `kvobjSetExpire` realloc (object.c:272, from P7 first-expire on a key created without the expire slot — i.e. every `EXPIRE` on a fresh key with key length < 128), `keyMetaSetMetadata` (keymeta.c:838), RENAME/MOVE re-add (db.c:2238/2332 → P3 → P10). Must be bypassed when the source is owed (clone instead of steal). The bg thread reading `ptr == NULL` would crash.
3. **`dbSetValue` in-place header swap** (db.c:636-652) rewrites `type/encoding/ptr` of the original kvobj and keeps it in the dict. Any SET over an existing RAW string, GCRA update, SPOP-count, INCRBYFLOAT, `dbUnshareStringValue` decode of an INT/EMBSTR goes here. Bypass when owed.
4. **`keyMetaTransition` writes reset values into the OLD header's metadata slots** (keymeta.c:708) from P10 and P5 — the bg copies the whole allocation including META (spec §1: "copies the whole kvobj allocation") and `rdbSaveKeyMetadata` (rdb.c:1614) serializes them; a reset mid-copy loses module metadata in the snapshot.
5. **Header word is a shared bitfield** (object.h:100-112): `refcount++/--` (P26) on the original from P5/P6/RENAME/MOVE are RMWs of the word holding `type/encoding/lru/metabits`. Not a correctness problem for the *bits* read, but a C-level data race; requires the copier to read the word with a relaxed atomic load and the mutator side to avoid touching originals entirely in owed mode (recommended) rather than relying on "ATOMIC".
6. **Value-level in-place writes bypass every db.c primitive.** APPEND/SETRANGE/SETBIT/BITFIELD/PFADD/PFCOUNT/INCR-INT/HSET/HDEL/SADD/SREM/ZADD/ZREM/LPUSH/LPOP/LSET/LTRIM/LINSERT/LREM/ARMSET… write `o->ptr` and the buffer directly after `lookupKeyWrite`. The only central choke point is **`lookupKeyWrite*`/`lookupKey(flags & LOOKUP_WRITE)`** (db.c:279) — it must return a clone (with a duplicated SNAP_NOFORK value) for owed keys. Read-lookups that later mutate (PFCOUNT hyperloglog.c:1742; HGET lazy field expiry; HRANDFIELD; `RM_OpenKey` READ followed by `RM_StringDMA(WRITE)`? — DMA WRITE requires the key opened WRITE, but `RM_StringDMA` with READ mode on a non-RAW value still calls `dbUnshareStringValue` module.c:4745-4746 → P5 → header swap on a **read-opened** key) are the exceptions that need their own SUSPEND or forced upgrade.
7. **Raw pointers held across calls that may clone.** `lmoveGenericCommand` holds `kvsrc`/`kvdst` (t_list.c:1210-1240) across pop→push→delete; `hashTypeSetExInit` stores `ex->hashObj` (t_hash.c:1208) and `hashTypeSetExDone` uses it for `estore*`; module `RedisModuleKey->kv` is cached from `RM_OpenKey` (module.c:4132) and refreshed only where the code already knows about realloc (`RM_SetExpire` 4328 `key->kv = setExpire(...)`, `RM_StringDMA`/`Truncate`), but **not** after `RM_SetKeyMeta` on a *different* open handle of the same key, nor after any nested command run via `RM_Call` that clones the key. `KSN_INVALIDATE_KVOBJ` (server.h:3908, just `o = NULL`) marks the places the tree already treats as "pointer may be stale". If the clone is created lazily on first write, everything the command obtained from `lookupKeyWrite` is already the clone, so the hazard is limited to code that looked up with READ and then writes (list above) and to blocking/module code re-looking-up by name (safe: name lookup consults the temp dict).
8. **`dictScan` on the bg thread vs. main-thread `_dictRehashStepIfNeeded` on PENDING slots.** `snapScanFreezeStore` pauses rehash for the whole store up-front (snapshot.c:347) — good — but `dictPauseRehashing` is a counter on the dict; `dictTwoPhaseUnlinkFind` (dict.c:958/978) and safe iterators (dict.c:1153/1179) pause/resume too and are balanced, so no conflict; `kvstoreMoveDict` and `emptyDbAsync` swap the `dict*` out from under the freeze → ABORT_SAVE only.
9. **Deferred frees on other threads.** `freeObjAsync` (bio) and `tryDeferFreeClientObject` (IO) free the *replaced value* or the *kvobj*; with M1 "defer all frees to save-end" the originals are never freed during the scan, but clones replaced by later clones may be; those go to bio/IO threads and must use the class-aware SNAP_NOFORK free path (`snapNoforkFree` / `snapSdsFree` etc.), i.e. `decrRefCount` (object.c:643-690: `zfree(alloc)`, `freeStringObject`…) itself must become class-aware for kvobj headers and SNAP_NOFORK values — an allocator-level blocker, not a write-path one, but it is where every free path converges (`lazyfreeFreeObject` lazyfree.c:16, `freeDeferredObject` networking.c:1717).
10. **Eviction semantics during a save.** With tombstones and deferred frees, `performEvictions` cannot reclaim memory from owed slots (§6) — either suspend eviction for owed slots (and accept over-maxmemory until the slot merges) or abort the save at the hard watermark; otherwise the eviction loop's `mem_freed` stays 0.
11. **Active subexpire's `ebExpire` writes `trash=1` before consulting the callback** (ebuckets.c:377/691) — a pure "skip" is not expressible without touching the item; needs an ebuckets change or SUSPEND of `activeSubexpiresCycle` for owed slots by *not calling* `estoreActiveExpire` on slots that are owed (check at expire.c:221 using the slot index, which is available: `activeSubexpires(db, slot, …)`). Recommended: skip whole owed slots there.
12. **Lazy field expiry on HGET-family reads** (t_hash.c:782/806) mutates a SNAP_NOFORK listpackEx through a READ lookup; must be suspended (return EXPIRED without delete) for owed keys.
13. **PFCOUNT is a read command that runs P5 (`dbUnshareStringValue`) and writes the value.**
14. **Direct header writes without a primitive (complete list from grep):** `o->ptr =` in t_list.c:53/91/159/163/197/343/361/385/423/463/472/964/965; t_set.c:93-94/116/161/175/204/240/260/267/453/534-535/559-560/584/591/932/987/1456/1536; t_zset.c:1489-1490/1513-1514/1647-1648/1661/1779/1883/2220-2226; t_hash.c:955/1224/1300/1647/1653-1654/1681-1682/1738-1739/1778/1792/1847; t_string.c:637/921/1350/1429; bitops.c:803; hyperloglog.c:673/716; t_array.c:1698; module.c:4782/4787; `o->encoding =` at the conversion sites above; `val->lru =`/`o->lru =` at db.c:58/66/322/654, object.c:1665/1682; `val->ptr = NULL` object.c:303; META writes at object.c:277, keymeta.c:708/802/846/851, keymeta.h:155, db.c:450/546 (memcpy on new kvobj), `keyMetaResetModuleValues` keymeta.c:921. Plus the `dbSetValue` swap db.c:636-652 and `activeDefragKvobj` defrag.c:1096 (`kvNew->ptr`).
15. **`rdbSaveDb` is not reusable as-is by the bg thread**: it writes `updateSlotAllocSize` (rdb.c:1839-1841) when memory tracking is on, calls `dismissObject` (child only, guarded), reads `lpt->meta` (rdb.c:1241), and iterates via `kvstoreIteratorNext` (which is a *safe* iterator — `dictInitSafeIterator` pauses rehash on the main thread's dict from another thread). The bg loop must be its own (`dictScan`-based, as snapshot.c:370 already is) and call only the pure encoders (`rdbSaveKeyValuePair` minus alloc-size bookkeeping).
16. **Module keymeta `unlink` callbacks receive a pointer into the original's META** (keymeta.c:315 `conf.unlink(&ctx, pMeta)`) during P5/P6 and may write it; in owed mode the callback should be given the clone's slot (or the write deferred).
17. **`SWAPDB` / `dbSwapDatabases` does not kill the child today** and neither does FLUSHDB (only FLUSHALL, db.c:1225) — both must ABORT the hybrid save explicitly (spec §7.5).

---

## Coverage checklist (files read)

- src/db.c (lookupKey, dbAddInternal/dbAdd/dbAddByLink/dbAddRDBLoad, dbSetValue/dbReplaceValue*/setKey*, dbGenericDelete family, dbUnshareStringValue*, emptyDbStructure/emptyData, keyModified/updateLRM/updateLFU, histograms, flushAllDataAndResetRDB, delGenericCommand/delexCommand, scanCallback, dbRandomKey, renameGenericCommand, moveCommand, copyCommand, dbSwapDatabases/swapMainDbWithTempDb/swapdbCommand, removeExpire/setExpire/setExpireByLink/getExpire, deleteKeyAndPropagate, expireIfNeeded, dbExpand, dbFind*)
- src/object.c (kvobjMetaRef, kvobjCreate, createObject, initObjectLRUOrLFU, kvobjCreateEmbedString, kvobjGetKey/GetExpire/SetExpire, kvobjSet, incrRefCount/decrRefCount, trimStringObjectIfNeeded, tryObjectEncodingEx, objectSetLRUOrLFU, kvobjCommandLookup, objectCommand); src/object.h (struct redisObject)
- src/keymeta.c (keyMetaOnUnlink, keyMetaOnFree, keyMetaTransition, keyMetaSetMetadata, keyMetaResetModuleValues), src/keymeta.h
- src/estore.c (all), src/ebuckets.c (ebExpire/ebRemove ExpireMeta writes), src/ebuckets.h (ExpireMeta)
- src/kvstore.c (find/set/add/delete/two-phase, TryResizeDicts, IncrementallyRehash, MoveDict, LUTDefrag, Scan), src/dict.c (_dictRehashStep, _dictBucketRehash, dictFind/dictFindLink, dictSetKeyAtLink, _dictRehashStepIfNeeded, dict_can_resize)
- src/server.c (databasesCron, updateDictResizePolicy, hasActiveChildProcess), src/server.h (LOOKUP_*, SETKEY_*, DB_FLAG_*, listpackEx, KSN_INVALIDATE_KVOBJ)
- src/t_hash.c (listpackEx helpers, hashTypeTryConversion, hashTypeGetFromListpack/HashTable, hashTypeGetValue, hashTypeExists, hashTypeSet, hashTypeSetEx/Init/Done, initDictExpireMetadata, hashTypeDelete, hashTypeLookupWriteOrCreate, hashTypeConvert*, hashTypeExpire, hashTypeExpireIfNeeded, hashTypeGetMinExpire, hfieldPersist, onFieldExpire, hashGetExpireMeta, HSET/HSETEX/HGETDEL/HGETEX/HDEL/HEXPIRE/HPERSIST bodies)
- src/expire.c (activeExpireCycleTryExpire, expireScanCallback, activeSubexpiresCb, activeSubexpires, activeSubexpiresCycle, activeExpireCycle, expireGenericCommand, persistCommand, touchCommand)
- src/evict.c (evictionPoolPopulate, LFUDecrAndReturn, performEvictions deletion path)
- src/defrag.c (activeDefragKvobj, defragKey, defragLaterStep, defragStageExpiresKvstore, activeDefragSubexpiresOB, defragStageSubexpires, activeDefragTimeProc gating, activeDefragCycle)
- src/lazyfree.c, src/bio.h, src/networking.c (freeDeferredObject/tryDeferFreeClientObject), src/iothread.c, src/memory_prefetch.c, src/blocked.c
- src/module.c (moduleDelKeyIfEmpty, moduleInitKey, RM_OpenKey, moduleCloseKey, RM_DeleteKey/UnlinkKey, RM_SetExpire/SetAbsExpire, RM_SetKeyMeta, RM_StringSet/DMA/Truncate, RM_ModuleTypeSetValue/ReplaceValue, RM_SetLRU/SetLFU, GIL functions, RM_TrimStringAllocation)
- src/t_string.c (SET/GETEX/GETDEL/GETSET/SETRANGE/MSET*/INCR*/INCRBYFLOAT/INCREX/APPEND), src/bitops.c, src/hyperloglog.c, src/sort.c
- src/t_list.c, src/t_set.c, src/t_zset.c, src/t_stream.c (primitive maps, conversion bodies, IDMP tracking + cron), src/geo.c, src/quicklist.c (compress macros)
- src/t_array.c, src/gcra.c, src/vector.c (header only — generic vector helper, not a data type)
- src/hotkeys.c
- src/cluster.c (restoreCommand), src/cluster_asm.c (import client, pre-expand, asmTrimSlots, background trim, active trim)
- src/debug.c (subcommand inventory, reload/loadaof/populate/object/digest-value)
- src/rdb.c (rdbSaveObject LISTPACK_EX, rdbSaveKeyValuePair, rdbSaveDb loop, load key-add path, kvstoreDictExpand), src/aof.c (rewriteAppendOnlyFileRio)
- src/replication.c (full-sync load, killRDBChild), src/eval.c / functions.c / script_lua.c / multi.c (grep only — no db primitives)
- src/snapshot.c, src/snapshot.h (current state of the branch)
- poc-hybrid-bgsave-spec.md §1, §4, §5, §7


---

## Reviewer addendum (implementation state after TASK-1.2 / 1.3 / 1.4, 2026-09-20)

Reviewed line by line against the tree. The primitive inventory, the command tables and the treatment proposal stand. Corrections and decisions taken since the audit was written:

- **Blocker 9 (frees on bio/IO threads must be class-aware) is resolved at the allocator level.** The SNAP_NOFORK arena is now backed by one reserved DONTFORK address range (zmalloc.c), so `zmalloc_is_nofork(p)` is a range check and the generic `zfree`/`zrealloc`/`zfree_with_size`/`zfree_no_tcache` keep arena memory in the arena through a per-thread explicit tcache. `decrRefCount`, `lazyfreeFreeObject`, `freeDeferredObject`, sds/listpack/intset code need no changes. What remains from blocker 9 is only the *timing* rule: originals of owed slots are not freed until merge (M1: save end).
- **`lookupKeyWrite*` returns the temp-dict clone** is adopted as the central hook design for TASK-1.6/1.7 (eager copy of SNAP_NOFORK values; shared `ptr` for SNAP_FORK values). The direct `o->ptr =` list in §7.14 becomes the regression list for the harness.
- **listpackEx**: the struct is now arena-born (`listpackExCreate` -> `snapNoforkCalloc`) and is never moved, because `lpt->meta` is linked into `db->subexpires`. A LISTPACK_EX key is served by the background thread whenever the struct is in the arena. Blocker 1 stays open for TASK-1.7/1.8 exactly as described: clones need their own struct, `estoreRemove(original)` is deferred to merge, active subexpire skips owed slots, and the serializer computes the min TTL from the listpack tuples (`listpackExGetMinExpire`) rather than reading `meta`.
- **Headers in the arena make every fork child unable to read the keyspace.** Stock BGSAVE, `rdbSaveToSlavesSockets` and AOF rewrite are refused while `bgsave-hybrid yes` until the hybrid path exists (TASK-1.9; diskless sync and AOF rewrite are M2). `hasActiveChildProcess()`-based gates (LRU-on-read, defrag, cron rehash) keep holding because the hybrid save still forks.
- snapshot.c line references in this document are stale (the file has grown); function names are stable: `snapScanKeyIsOwed`, `snapScanStart` (freezes with `dictPauseRehashing` + `dictPauseAutoResize`), `snapKeyClass`, `snapPlaceValue`.
- **Eviction (blocker 10)**: decision deferred to TASK-1.7; the recommendation (suspend eviction for owed slots, rely on the hard watermark abort) is the default unless measurement says otherwise.


---

## Hook locations (as implemented, 2026-09-21)

| Treatment (§6) | Implemented where |
|---|---|
| CLONE_HEADER / CLONE_HEADER_VALUE for every write path | `kvstoreDictFindLink`, `kvstoreDictTwoPhaseUnlinkFind`, `kvstoreDictDelete`, `kvstoreDictAddRaw` (kvstore.c, overlay): the first write-link lookup of a frozen key calls `snapOverlayCloneKey` (snapshot.c) and returns the clone's link; writers never see a frozen entry. `lookupKey` (db.c) uses the link lookup only for `LOOKUP_WRITE`. |
| Header copy / value copy-or-share, expires + subexpires repointing | `snapOverlayCloneKey` (snapshot.c): whole allocation copy, EMBSTR ptr rebased; arena-resident sds/listpack/intset/listpackEx deep-copied; multi-allocation and outside-arena values shared. |
| TOMBSTONE | `kvstoreDictTwoPhaseUnlinkFree` / `kvstoreDictDelete` on a frozen dict (state flag `OV_STATE_TOMB`). |
| ADD_TEMP | `kvstoreDictSetAtLink(newItem)` / `kvstoreDictAddRaw` on a frozen dict. |
| Merge and original release | `kvstoreOverlayMerge` (kvstore.c) called from `snapScanCompleteSlot`; `snapOverlayFreeOriginal` (header-only when shared, `kvobjFreeValueOnly` otherwise). |
| SUSPEND: active hash-field expiry | `activeSubexpiresCycle` (expire.c) returns while `snapSaveInProgress()`. |
| SUSPEND: lazy field expiry on read, HRANDFIELD expiry | `hashTypeGetValue`, `hashTypeExpireIfNeeded` (t_hash.c) via `snapKeyIsOwed`. |
| ABORT_SAVE | `emptyData`, `dbSwapDatabases` (db.c), `killRDBChild` (rdb.c) call `snapSaveAbort`. |
| NOOP: LRU/LFU on read, defrag, cron rehash/resize | unchanged, all gated by `hasActiveChildProcess()` (the hybrid save forks). `updateLRM` writes the writer's kvobj, i.e. a clone for owed keys. |
| Conversions adopting an arena buffer | `snapValueDetachFromArena` before `quicklistAppendListpack` in `listTypeTryConvertListpack` (t_list.c). |
| Debug assertion (invariant 1) | `snapDebugAssertNotFrozenOriginal` at `dbSetValue`, `setExpireByLink`, `removeExpire`, `dbGenericDelete`, `keyModified`, `dbUnshareStringValueByLink` (DEBUG_ASSERTIONS builds). |
| Serializer reads of `lpt->meta` | `rdbSaveObject` uses `hashTypeGetMinExpire(o, 1)` on the scan thread (`snapOnScanThread`). |

Blockers still open: eviction during a save only tombstones (no memory reclaimed until merge); cluster ASM slot import/trim during a save is not guarded; diskless sync and AOF rewrite are refused in hybrid mode (M2).

### Additions found by the harness (2026-09-21)

| Path | Treatment |
|---|---|
| SET overwrite, in-place swap path of `dbSetValue` (old kvobj kept, `ptr` swapped) | `snapPlaceValue(kvNew)` after the swap: the adopted value follows the placement policy |
| `setTypeConvertAndExpand` to listpack, `zsetConvertAndExpand` to listpack | `snapPlaceValue` after installing the fresh listpack |
| Async delete (UNLINK) of an owed key | key passed explicitly to `kvstoreDictTwoPhaseUnlinkFreeKey`: the entry's value is already detached |
| Slot empty at freeze | never frozen (DONE from the start); adds go to a real dict created on demand |
| Cluster memory tracking on the overlay | temp dict destructor accounts on the real dict; a clone takes over its original's `alloc_size` |
| Scan-thread reads of `server.*` | `rdb_key_save_delay`, `maxmemory_policy`, `rdb_compression` copied per save; LRU clock computed with `getLRUClock()`; `hashTypeNext` only reads `allow_access_expired` when skipping was requested |
| Arena-free mode | single-allocation values are always copied on clone (the scan thread classifies by reading the allocation) |
| Write lookup on a frozen slot, key already scanned (kvBits.save_gen == current) | `kvstoreDictFindLink` returns the frozen entry: in-place write, no clone (`snapOverlayKeyScanned`) |
| Delete of an already-scanned frozen key | tombstone via a header-only clone that owns the value (`snapOverlayCloneKey` with the scanned check) |
