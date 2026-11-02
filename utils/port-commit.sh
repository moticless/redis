#!/bin/sh
# Port commits across the src/ folder move (utils/layout-map.txt).
#
# Usage: port-commit.sh [--dry-run] <commit|A..B>...
#
# Each commit is exported with `git format-patch`, its paths are rewritten
# through the layout map to match the layout of the current checkout, and the
# patch is applied with `git am -3`. The direction is detected per commit:
#
#   old-layout commit -> new-layout checkout   (forward port)
#   new-layout commit -> old-layout checkout   (backport to a release branch)
#   same layout on both sides                  (plain `git am -3`)
#
# This gives the same result as a cherry-pick between branches that share a
# layout, including the same conflicts. A plain `git cherry-pick` across the
# move relies on rename detection instead, which misses files that diverged a
# lot between branches; those then show up as modify/delete conflicts.
#
# On a conflict, `git am` stops: resolve, `git add`, then `git am --continue`
# (or `git am --abort`). Remaining commits are not applied after a stop.
#
# The script works on the repository in the current directory, so it can be run
# from a release-branch checkout that does not contain it, e.g.:
#   git show unstable:utils/port-commit.sh > /tmp/port-commit.sh
#   sh /tmp/port-commit.sh <commit>
# The map is read from $LAYOUT_MAP if set, else from HEAD, else from the commit
# being ported.
set -e

DRYRUN=0
REVS=""
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRYRUN=1 ;;
        -h|--help) sed -n '2,27p' "$0"; exit 0 ;;
        -*) echo "unknown option: $arg" >&2; exit 2 ;;
        *..*) REVS="$REVS $(git rev-list --reverse --no-merges "$arg")" ;;
        *) REVS="$REVS $(git rev-parse --verify "$arg^{commit}")" ;;
    esac
done
[ -n "$REVS" ] || { echo "usage: $0 [--dry-run] <commit|A..B>..." >&2; exit 2; }

# A tree uses the new layout when src/core/server.h exists in it.
is_new() { git cat-file -e "$1:src/core/server.h" 2>/dev/null; }

TMP=$(mktemp -d "${TMPDIR:-/tmp}/port-commit.XXXXXX")
trap 'rm -rf "$TMP"' EXIT

if is_new HEAD; then target=new; else target=old; fi

for rev in $REVS; do
    short=$(git rev-parse --short "$rev")
    if is_new "$rev^"; then from=new; else from=old; fi

    if [ -n "${LAYOUT_MAP:-}" ]; then
        cp "$LAYOUT_MAP" "$TMP/map"
    elif git cat-file -e HEAD:utils/layout-map.txt 2>/dev/null; then
        git show HEAD:utils/layout-map.txt > "$TMP/map"
    elif git cat-file -e "$rev:utils/layout-map.txt" 2>/dev/null; then
        git show "$rev:utils/layout-map.txt" > "$TMP/map"
    elif [ "$from" != "$target" ]; then
        echo "error: no utils/layout-map.txt in HEAD or $short" >&2; exit 1
    fi

    git format-patch -1 --stdout --binary "$rev" > "$TMP/patch"
    if [ "$from" = old ] && [ "$target" = new ]; then dir=forward
    elif [ "$from" = new ] && [ "$target" = old ]; then dir=reverse
    else dir=none; fi

    if [ "$dir" != none ]; then
        # Rewrite only path tokens in diff headers; content lines are untouched.
        awk -v map="$TMP/map" -v dir="$dir" '
            BEGIN {
                while ((getline l < map) > 0) {
                    if (l ~ /^(#|keep )/ || l == "") continue
                    split(l, a, " ")
                    if (dir == "forward") m[a[1]] = a[2]; else m[a[2]] = a[1]
                }
            }
            /^(diff --git|---|\+\+\+|rename from|rename to|copy from|copy to) / {
                n = split($0, f, " "); out = ""
                for (i = 1; i <= n; i++) {
                    w = f[i]; p = ""
                    if (w ~ /^[ab]\//) { p = substr(w, 1, 2); w = substr(w, 3) }
                    if (w in m) w = m[w]
                    out = out (i > 1 ? " " : "") p w
                }
                print out; next
            }
            { print }' "$TMP/patch" > "$TMP/patch.rw"
        mv "$TMP/patch.rw" "$TMP/patch"
    fi

    if [ "$target" = new ]; then
        # New files added directly in src/ are not placed by the map.
        grep -E '^\+\+\+ b/src/[^/]+$' "$TMP/patch" | while read -r _ f; do
            f=${f#b/}
            if ! git cat-file -e "HEAD:$f" 2>/dev/null; then
                echo "note: $short adds $f in src/; place it in a folder (layout-map.txt + CMakeLists.txt)" >&2
            fi
        done
    fi
    if [ "$from" != "$target" ] && grep -qE '^diff --git a/src/Makefile ' "$TMP/patch"; then
        echo "note: $short changes src/Makefile, which differs between the layouts; check that hunk" >&2
    fi

    echo "porting $short ($from layout -> $target layout): $(git log -1 --format=%s "$rev")"
    if [ "$DRYRUN" = 1 ]; then
        cat "$TMP/patch"
    else
        git am -3 -q "$TMP/patch"
    fi
done
