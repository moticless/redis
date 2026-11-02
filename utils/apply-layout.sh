#!/bin/sh
# Apply the source layout map (utils/layout-map.txt) with `git mv`.
#
# Usage: utils/apply-layout.sh [--reverse] [--dry-run] [--check]
#
#   (default)   move every "old new" entry from old to new
#   --reverse   move from new back to old (porting fixes to old release lines)
#   --dry-run   print the git mv commands without running them
#   --check     exit non-zero if any src/ root file is not covered by the map
#
# The script is idempotent: entries that are already at their destination are
# skipped. It only renames files, it never edits them. Downstream branches do not
# need it to merge the move (see the comment in utils/layout-map.txt).
set -e

cd "$(dirname "$0")/.."
MAP=utils/layout-map.txt
REVERSE=0
DRYRUN=0
CHECK=0
for arg in "$@"; do
    case "$arg" in
        --reverse) REVERSE=1 ;;
        --dry-run) DRYRUN=1 ;;
        --check) CHECK=1 ;;
        *) echo "unknown option: $arg" >&2; exit 2 ;;
    esac
done

if [ "$CHECK" = 1 ]; then
    rc=0
    for f in $(git ls-files src | awk -F/ 'NF==2'); do
        if ! grep -q -E "^(keep $f|$f )" "$MAP"; then
            echo "not in $MAP: $f" >&2
            rc=1
        fi
    done
    exit $rc
fi

grep -v -E '^(#|keep |$)' "$MAP" | while read -r old new; do
    if [ "$REVERSE" = 1 ]; then src="$new"; dst="$old"; else src="$old"; dst="$new"; fi
    if [ -e "$dst" ] && [ ! -e "$src" ]; then
        continue
    fi
    if [ ! -e "$src" ]; then
        echo "warning: $src does not exist, skipping" >&2
        continue
    fi
    if [ -e "$dst" ]; then
        echo "error: both $src and $dst exist" >&2
        exit 1
    fi
    if [ "$DRYRUN" = 1 ]; then
        echo "git mv $src $dst"
    else
        mkdir -p "$(dirname "$dst")"
        git mv "$src" "$dst"
    fi
done
