# Source layout

`src/` is organized in folders by responsibility. Each folder has a
`CMakeLists.txt` that lists its sources with `redis_folder()`
(see `src/CMakeLists.txt`).

| Folder | Contents |
|---|---|
| `platform/` | Platform and compiler headers: `fmacros.h`, `config.h`, `atomicvar.h`, `debugmacro.h`, `version.h` |
| `util/` | Self-contained helpers: allocator wrapper (`zmalloc`), hashing and CRCs, `lzf`, random, time, string and number utilities, `memtest`, `syscheck` |
| `ds/` | Data structures shared by several types: `sds`, `dict`, `adlist`, `listpack`, `ziplist`, `zipmap`, `rax`, `kvstore`, `fwtree`, `ebuckets`, `mstr`, `vector` |
| `net/` | Event loop (`ae.c` and its backends) and socket helpers (`anet`) |
| `core/` | Server core: `server.c/h`, networking and the connection layer, keyspace (`db`, `object`, `expire`, `evict`, `lazyfree`), config, ACL, commands table, pub/sub, tracking, blocking, debug, defrag, latency and slowlog |
| `types/<type>/` | One folder per data type, holding its commands and the data structures only it uses: `string`, `list` (+ `quicklist`), `hash` (+ `hash_entry`), `set` (+ `set_entry`, `intset`), `zset` (+ `zset_btree`), `stream`, `geo` (+ `geohash`), `hll`, `bitmap` (+ `bitroar`), `array` (+ `sparsearray`), `gcra` |
| `persistence/` | RDB, AOF, `rio`, background I/O, and the `redis-check-rdb` / `redis-check-aof` entry points (linked into `redis-server`) |
| `replication/` | Replication |
| `cluster/` | Cluster |
| `scripting/` | Lua scripting and functions |
| `moduleapi/` | The module API implementation (`module.c`) |
| `sentinel/` | Sentinel |
| `tools/` | `redis-cli` (`tools/cli/`) and `redis-benchmark` (`tools/benchmark/`); they never include `server.h` |
| `misc/` | `LOLWUT` and `sparkline` |

Files that stay in `src/` itself:

- `redismodule.h`: the module API header, at the path module authors build against.
- `solarisfixes.h`: included by path from `deps/lua/src/lua_cjson.c`.
- `commands/`: the command JSON files that `utils/generate-command-code.py` turns into `core/commands.def`.
- `modules/`: example modules.

## Rules

- **Placement.** A data structure lives in a type folder only if that type is
  its only user; otherwise it goes in `ds/`.
- **Includes.** Every folder is on the quote-only include path (`-iquote`), so
  `#include "server.h"` works from any folder, and `<...>` includes still
  resolve to the bundled dependencies (redis-cli relies on this for hiredis'
  `<sds.h>`).
- **Moves.** Files move only through `utils/layout-map.txt` and
  `utils/apply-layout.sh`, as pure renames, so downstream branches can merge
  the move as-is and git carries their changes to the new paths.
  `utils/apply-layout.sh --check` fails if a file in `src/` itself is missing
  from the map.
- **Porting single commits across the move.** `utils/port-commit.sh <commit>`
  rewrites the commit's paths through the map to match the current checkout
  (either direction) and applies it with `git am -3`. It gives the same result,
  and the same conflicts, as a cherry-pick between branches with one layout;
  a plain `git cherry-pick` relies on rename detection, which misses files
  that diverged a lot between branches.
