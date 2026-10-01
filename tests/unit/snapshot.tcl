start_server {tags {"snapshot"} overrides {bgsave-hybrid yes}} {
    test {save-small-max-bytes: default value and CONFIG SET with units} {
        assert_equal 4096 [lindex [r config get save-small-max-bytes] 1]
        r config set save-small-max-bytes 8kb
        assert_equal 8192 [lindex [r config get save-small-max-bytes] 1]
        r config set save-small-max-bytes 4096
        assert_equal 4096 [lindex [r config get save-small-max-bytes] 1]
    }

    test {INFO memory exposes SNAP_NOFORK arena stats} {
        set info [r info memory]
        assert_match {*allocator_allocated_nofork:*} $info
        assert_match {*allocator_active_nofork:*} $info
        assert_match {*allocator_resident_nofork:*} $info
        assert_match {*allocator_frag_bytes_nofork:*} $info
        assert_match {*snap_nofork_arena_available:*} $info
        # On Linux + jemalloc the arena must be available.
        if {[string match {*jemalloc*} [s mem_allocator]] && $::tcl_platform(os) eq "Linux"} {
            assert_equal 1 [s snap_nofork_arena_available]
        }
    }
}

proc snap_placement {{db ""}} {
    if {$db eq ""} { return [dict create {*}[r debug snap-placement]] }
    return [dict create {*}[r debug snap-placement $db]]
}

# The hybrid save path runs with or without the SNAP_NOFORK arena (sanitizer
# builds use libc and have no arena); placement checks only make sense with it.
proc snap_arena_available {} { expr {[s snap_nofork_arena_available] == 1} }
proc snap_arena_test {name body} {
    if {[snap_arena_available]} { uplevel 1 [list test $name $body] }
}
proc snap_assert_placement_clean {} {
    if {![snap_arena_available]} return
    set st [snap_placement]
    if {[dict get $st violations] || [dict get $st headers_outside] ||
        [dict get $st drift_nofork_outside] || [dict get $st drift_fork_inside]} {
        fail "placement: $st; drifting keys: [r debug snap-drift]"
    }
}

start_server {tags {"snapshot placement"} overrides {bgsave-hybrid yes save "" repl-diskless-sync no}} {
    if {1} {
        r config set save-small-max-bytes 4096

        proc populate_all_types {} {
            r flushall
            r set str:small "hello"
            r set str:embstr [string repeat x 30]
            r set str:raw [string repeat x 200]
            r set str:big [string repeat x 10000]
            r set str:int 12345
            r hset hash:lp f1 v1 f2 v2
            r hset hash:lpex f1 v1 f2 v2
            r hexpire hash:lpex 100000 FIELDS 1 f1
            for {set i 0} {$i < 600} {incr i} { r hset hash:ht f$i v$i }
            r sadd set:intset 1 2 3 4 5
            r sadd set:lp a b c
            for {set i 0} {$i < 600} {incr i} { r sadd set:ht m$i }
            r zadd zset:lp 1 a 2 b
            for {set i 0} {$i < 300} {incr i} { r zadd zset:sl $i m$i }
            r rpush list:lp a b c
            for {set i 0} {$i < 2000} {incr i} { r rpush list:ql [string repeat y 100] }
            r xadd stream:x * f v
        }

        snap_arena_test {SNAP placement: all types placed, no violations} {
            populate_all_types
            assert_equal listpack [r object encoding hash:lp]
            assert_equal listpackex [r object encoding hash:lpex]
            assert_equal hashtable [r object encoding hash:ht]
            assert_equal intset [r object encoding set:intset]
            assert_equal listpack [r object encoding set:lp]
            assert_equal hashtable [r object encoding set:ht]
            assert_equal listpack [r object encoding zset:lp]
            assert_equal skiplist [r object encoding zset:sl]
            assert_equal listpack [r object encoding list:lp]
            assert_equal quicklist [r object encoding list:ql]
            set st [snap_placement]
            assert_equal 1 [dict get $st arena_available]
            assert_equal [r dbsize] [dict get $st keys]
            assert_equal [dict get $st keys] [dict get $st headers_in_arena]
            assert_equal 0 [dict get $st headers_outside]
            assert_equal 0 [dict get $st violations]
            assert_equal 0 [dict get $st drift_nofork_outside]
            assert_equal 0 [dict get $st drift_fork_inside]
            assert_equal 0 [dict get $st dict_tables_outside]
            assert {[dict get $st dict_tables_in_arena] >= 2}
            # NOFORK: str:small str:embstr str:raw str:int hash:lp hash:lpex set:intset set:lp zset:lp list:lp = 10
            assert_equal 10 [dict get $st class_nofork]
            # FORK: str:big hash:ht set:ht zset:sl list:ql stream:x = 6
            assert_equal 6 [dict get $st class_fork]
        }

        snap_arena_test {SNAP placement: encoding conversions keep placement consistent} {
            populate_all_types
            # listpack -> hashtable (hash), intset -> listpack -> hashtable (set), listpack -> skiplist (zset)
            for {set i 0} {$i < 600} {incr i} { r hset hash:lp g$i v }
            assert_equal hashtable [r object encoding hash:lp]
            r sadd set:intset notanint
            assert_equal listpack [r object encoding set:intset]
            for {set i 0} {$i < 600} {incr i} { r sadd set:intset s$i }
            assert_equal hashtable [r object encoding set:intset]
            for {set i 0} {$i < 300} {incr i} { r zadd zset:lp $i n$i }
            assert_equal skiplist [r object encoding zset:lp]
            # quicklist -> listpack (list shrink) and listpack -> quicklist (list grow)
            for {set i 0} {$i < 1998} {incr i} { r lpop list:ql }
            assert_equal listpack [r object encoding list:ql]
            for {set i 0} {$i < 2000} {incr i} { r rpush list:lp [string repeat y 100] }
            assert_equal quicklist [r object encoding list:lp]
            set st [snap_placement]
            assert_equal 0 [dict get $st violations]
            assert_equal 0 [dict get $st headers_outside]
            assert_equal 0 [dict get $st drift_nofork_outside]
            assert_equal 0 [dict get $st drift_fork_inside]
            # NOFORK now: str:small str:embstr str:raw str:int hash:lpex set:lp list:ql = 7
            assert_equal 7 [dict get $st class_nofork]
        }

        snap_arena_test {SNAP placement: string growth past the threshold relocates the value} {
            r flushall
            r set k [string repeat a 100]
            assert_equal 1 [dict get [snap_placement] class_nofork]
            r append k [string repeat b 5000]
            set st [snap_placement]
            assert_equal 1 [dict get $st class_fork]
            assert_equal 0 [dict get $st drift_fork_inside]
            assert_equal 0 [dict get $st violations]
            assert_equal [string repeat a 100][string repeat b 5000] [r get k]
            r set k2 [string repeat a 100]
            r setrange k2 6000 z
            r set k3 [string repeat a 100]
            r setbit k3 50000 1
            set st [snap_placement]
            assert_equal 3 [dict get $st class_fork]
            assert_equal 0 [dict get $st drift_fork_inside]
            assert_equal 0 [dict get $st violations]
        }

        snap_arena_test {SNAP placement: DEBUG RELOAD, DUMP/RESTORE and RENAME re-place values} {
            populate_all_types
            set before [snap_placement]
            r debug reload
            set after [snap_placement]
            assert_equal [dict get $before class_nofork] [dict get $after class_nofork]
            assert_equal [dict get $before class_fork] [dict get $after class_fork]
            assert_equal 0 [dict get $after violations]
            assert_equal 0 [dict get $after drift_nofork_outside]
            set dump [r dump hash:lp]
            r restore hash:lp:restored 0 $dump
            r rename str:raw str:raw:renamed
            r copy set:intset set:intset:copy
            set st [snap_placement]
            assert_equal 0 [dict get $st violations]
            assert_equal 0 [dict get $st drift_nofork_outside]
            assert_equal 12 [dict get $st class_nofork]
        }

        snap_arena_test {SNAP placement: OBJECT ENCODING, MEMORY USAGE, DEBUG OBJECT unaffected} {
            populate_all_types
            assert_equal embstr [r object encoding str:embstr]
            assert_equal raw [r object encoding str:raw]
            assert_equal int [r object encoding str:int]
            assert {[r memory usage str:raw] > 200}
            assert_match {*encoding:raw*} [r debug object str:raw]
            assert_match {*encoding:listpack*} [r debug object hash:lp]
        }

        proc wait_hybrid_bgsave_done {} {
            wait_for_condition 300 100 {
                [s rdb_bgsave_in_progress] == 0 && [s snap_save_in_progress] == 0
            } else { fail "hybrid bgsave did not finish" }
        }

        proc populate_fillers {n} {
            for {set i 0} {$i < $n} {incr i} { r set filler:$i [string repeat f 60] }
        }

        test {Hybrid BGSAVE: static dataset saves and reloads identically} {
            populate_all_types
            populate_fillers 500
            set digest [debug_digest]
            r bgsave
            wait_hybrid_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            assert_equal 0 [s rdb_changes_since_last_save]
            r debug reload nosave
            assert_equal $digest [debug_digest]
            snap_assert_placement_clean
        }

        test {Hybrid BGSAVE: INFO exposes progress while the save runs, CONFIG SET rdb-key-save-delay reaches the scan thread} {
            populate_fillers 3000
            r config set rdb-key-save-delay 100000   ;# 100 ms per key: 3000 keys would take 5 minutes
            r bgsave
            wait_for_condition 50 50 {[s snap_save_in_progress] == 1} else { fail "hybrid save not started" }
            assert {[s snap_save_slots_total] >= 1}
            set t0 [clock milliseconds]
            r config set rdb-key-save-delay 0
            wait_hybrid_bgsave_done
            assert {[clock milliseconds] - $t0 < 5000}
            assert_equal ok [s rdb_last_bgsave_status]
            assert {[s snap_save_in_progress] == 0}
        }

        proc snap_main_table_size {} {
            regexp {main hash table\):\n table size: (\d+)} [r debug htstats 9] -> size
            return $size
        }

        test {Hybrid BGSAVE: keyspace dicts keep their bucket layout during a save and resize afterwards} {
            r flushall
            populate_fillers 2000
            wait_for_condition 100 20 {![string match "*rehashing target*" [r debug htstats 9]]} else { fail "rehash did not finish" }
            set size0 [snap_main_table_size]
            assert {$size0 >= 2048}
            r config set rdb-key-save-delay 3000   ;# 2000 keys x 3 ms: about 6 s
            r bgsave
            wait_for_condition 50 50 {[s snap_save_in_progress] == 1} else { fail "hybrid save not started" }
            # far more adds than an expand needs: with the layout locked the
            # table must not grow or start rehashing while the thread reads it
            for {set i 0} {$i < 6000} {incr i} { r set grow:$i x }
            r debug htstats 9   ;# exercised for the "rehashing target" check below
            assert_equal $size0 [snap_main_table_size]
            assert {![string match "*rehashing target*" [r debug htstats 9]]}
            assert_equal 1 [s snap_save_in_progress]
            assert_equal 8000 [r dbsize]
            r config set rdb-key-save-delay 0
            wait_hybrid_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            # unfrozen: the table catches up with its 8000 keys
            wait_for_condition 100 50 {[snap_main_table_size] > $size0 || [string match "*rehashing target*" [r debug htstats 9]]} else {
                fail "table did not resize after the save"
            }
            assert_equal 8000 [r dbsize]
            r flushall
        }

        test {Hybrid BGSAVE: writes during the save land in the overlay, snapshot keeps the fork instant} {
            populate_all_types
            populate_fillers 3000
            set digest [debug_digest]
            # slow enough that the writes below always race the scan, even on a
            # loaded CI box (the delay is reset to 0 once they are done)
            r config set rdb-key-save-delay 5000
            r bgsave
            wait_for_condition 50 50 {[s snap_save_in_progress] == 1} else { fail "hybrid save not started" }
            # mutate owed keys of every kind while the scan runs
            r set str:small changed
            r del str:raw
            r set brand:new 1
            r append str:embstr [string repeat z 6000]
            r hset hash:lp f3 v3
            r hset hash:lpex f9 v9
            r expire set:intset 1000
            r incr str:int
            r rpush list:lp d
            r rename zset:lp zset:renamed
            r setrange str:big 0 XYZ
            r sadd set:ht extra
            r zadd zset:sl 999 extra
            r xadd stream:x * g w
            r del filler:1
            r set filler:1 back
            r del filler:2
            # header-only clones of SNAP_FORK keys (value shared with the original)
            r expire hash:ht 1000
            r expire list:ql 1000
            r persist list:ql
            # encoding conversions on owed SNAP_NOFORK clones
            for {set i 0} {$i < 600} {incr i} { r hset hash:lp c$i v }
            assert_equal hashtable [r object encoding hash:lp]
            r sadd set:intset notanint
            for {set i 0} {$i < 600} {incr i} { r sadd set:intset s$i }
            assert_equal hashtable [r object encoding set:intset]
            for {set i 0} {$i < 300} {incr i} { r zadd zset:renamed $i n$i }
            assert_equal skiplist [r object encoding zset:renamed]
            for {set i 0} {$i < 2000} {incr i} { r rpush list:lp [string repeat y 100] }
            assert_equal quicklist [r object encoding list:lp]
            for {set i 0} {$i < 1998} {incr i} { r lpop list:ql }
            assert_equal listpack [r object encoding list:ql]
            # the live keyspace reflects the writes immediately
            assert_equal changed [r get str:small]
            assert_equal 0 [r exists str:raw]
            assert_equal 1 [r exists brand:new]
            assert_equal v3 [r hget hash:lp f3]
            assert_equal back [r get filler:1]
            assert_equal 0 [r exists filler:2]
            assert_equal 1 [s snap_save_in_progress]
            r config set rdb-key-save-delay 0
            wait_hybrid_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            # after the merge the live keyspace is unchanged
            assert_equal changed [r get str:small]
            assert_equal 0 [r exists str:raw]
            assert_equal 12346 [r get str:int]
            assert_equal v3 [r hget hash:lp f3]
            assert_equal v9 [r hget hash:lpex f9]
            assert {[r ttl set:intset] > 0}
            assert_equal 1 [r exists zset:renamed]
            assert_equal 0 [r exists zset:lp]
            assert_equal XYZ [string range [r get str:big] 0 2]
            assert_equal 1 [r sismember set:ht extra]
            assert_equal 999 [r zscore zset:sl extra]
            assert_equal 2 [r llen list:ql]
            assert_equal 2004 [r llen list:lp]
            assert_equal 603 [r hlen hash:lp]
            assert_equal 606 [r scard set:intset]
            assert_equal 302 [r zcard zset:renamed]
            assert_equal back [r get filler:1]
            assert_equal 0 [r exists filler:2]
            assert {[r ttl hash:ht] > 0}
            assert_equal -1 [r ttl list:ql]
            assert_equal 600 [r hlen hash:ht]
            snap_assert_placement_clean
            # the file holds exactly the fork-instant keyspace
            r debug reload nosave
            assert_equal $digest [debug_digest]
            assert_equal hello [r get str:small]
            assert_equal 1 [r exists str:raw]
            assert_equal 0 [r exists brand:new]
            assert_equal 1 [r exists filler:2]
        }

        foreach policy {allkeys-lru allkeys-lfu} {
            test "Hybrid BGSAVE: reads and writes during the save under $policy" {
                r config set maxmemory-policy $policy
                populate_all_types
                populate_fillers 3000
                set digest [debug_digest]
                r config set rdb-key-save-delay 300
                r bgsave
                wait_for_condition 50 50 {[s snap_save_in_progress] == 1} else { fail "hybrid save not started" }
                for {set i 0} {$i < 500} {incr i} { r get filler:$i; r hget hash:lp f1; r lindex list:ql 0 }
                r set str:small changed
                r incr str:int
                r config set rdb-key-save-delay 0
                wait_hybrid_bgsave_done
                assert_equal ok [s rdb_last_bgsave_status]
                assert_equal changed [r get str:small]
                r debug reload nosave
                assert_equal $digest [debug_digest]
                r config set maxmemory-policy noeviction
            }
        }

        test {Hybrid BGSAVE: active defrag enabled during a save} {
            populate_all_types
            populate_fillers 3000
            set digest [debug_digest]
            r config set active-defrag-ignore-bytes 1
            r config set active-defrag-threshold-lower 0
            r config set active-defrag-cycle-min 50
            if {[catch {r config set activedefrag yes} e]} {
                # defrag not supported on this allocator build
                r config set activedefrag no
            }
            r config set rdb-key-save-delay 300
            r bgsave
            wait_for_condition 50 50 {[s snap_save_in_progress] == 1} else { fail "hybrid save not started" }
            for {set i 0} {$i < 1000} {incr i} { r set filler:$i [string repeat g 70] }
            for {set i 1000} {$i < 1500} {incr i} { r del filler:$i }
            after 300
            r config set rdb-key-save-delay 0
            wait_hybrid_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            r config set activedefrag no
            snap_assert_placement_clean
            r debug reload nosave
            assert_equal $digest [debug_digest]
        }

        test {Hybrid BGSAVE: FLUSHALL during the save aborts it and the next save works} {
            populate_all_types
            populate_fillers 3000
            r config set rdb-key-save-delay 300
            r bgsave
            wait_for_condition 50 50 {[s snap_save_in_progress] == 1} else { fail "hybrid save not started" }
            r flushall
            assert_equal 0 [r dbsize]
            assert_equal 0 [s snap_save_in_progress]
            wait_hybrid_bgsave_done
            r config set rdb-key-save-delay 0
            r set after 1
            r bgsave
            wait_hybrid_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            r debug reload nosave
            assert_equal 1 [r get after]
            assert_equal 1 [r dbsize]
        }

        test {Hybrid BGSAVE: FLUSHDB during the save empties the live db at once and the save completes with the fork-instant keys} {
            foreach async {{} async} {
                r flushall
                populate_fillers 3000
                set digest [debug_digest]
                r config set rdb-key-save-delay 300
                r bgsave
                wait_for_condition 50 50 {[s snap_save_in_progress] == 1} else { fail "hybrid save not started" }
                set mem_full [s used_memory]
                r flushdb {*}$async
                # the live db is empty for everyone right away; the save goes on
                # over the detached fork-instant keyspace
                assert_equal 0 [r dbsize]
                assert_equal 1 [s snap_save_in_progress]
                r set after:flush 1
                assert_equal 1 [r dbsize]
                r config set rdb-key-save-delay 0
                wait_hybrid_bgsave_done
                assert_equal ok [s rdb_last_bgsave_status]
                # the detached keyspace is freed (on BIO) once the save is done with it
                wait_for_condition 100 50 {[s lazyfree_pending_objects] == 0 && [s used_memory] < $mem_full - 100000} else {
                    fail "flushed keyspace not released after the save"
                }
                r debug reload nosave
                assert_equal $digest [debug_digest]
                assert_equal 3000 [r dbsize]
                assert_equal 0 [r exists after:flush]
            }
        }

        test {Hybrid BGSAVE: SWAPDB during the save keeps the save running, keys stay under their fork-instant db} {
            r flushall
            populate_fillers 3000
            r select 10
            populate 100 ten: 40
            r select 9
            set digest [debug_digest]
            r config set rdb-key-save-delay 300
            r bgsave
            wait_for_condition 50 50 {[s snap_save_in_progress] == 1} else { fail "hybrid save not started" }
            r swapdb 9 10
            assert_equal 1 [s snap_save_in_progress]
            assert_equal 100 [r dbsize]            ;# db 9 now holds the former db 10
            r set swapped:probe 1                  ;# a write through the swapped db reaches the right kvstore
            r select 10
            assert_equal 3000 [r dbsize]
            r set filler:0 [string repeat s 60]    ;# in-place write on an owed key of the swapped store
            r select 9
            r config set rdb-key-save-delay 0
            wait_hybrid_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            # the dump holds the fork instant: 3000 fillers in db 9, 100 keys in db 10
            r debug reload nosave
            assert_equal $digest [debug_digest]
            assert_equal 3000 [r dbsize]
            r select 10
            assert_equal 100 [r dbsize]
            r select 9
            r flushall
        }

        test {Hybrid BGSAVE: disk-based full sync places values on the replica and matches the master} {
            populate_all_types
            set digest [debug_digest]
            start_server {overrides {bgsave-hybrid yes}} {
                r replicaof [srv -1 host] [srv -1 port]
                wait_for_condition 100 100 {
                    [s master_link_status] eq {up}
                } else { fail "replication not established" }
                wait_for_condition 50 100 {
                    [r dbsize] == [[srv -1 client] dbsize]
                } else { fail "replica did not receive all keys" }
                assert_equal $digest [debug_digest]
                if {[snap_arena_available]} {
                    set st [snap_placement]
                    assert_equal 0 [dict get $st violations]
                    assert_equal 0 [dict get $st headers_outside]
                    assert_equal 10 [dict get $st class_nofork]
                    assert_equal 6 [dict get $st class_fork]
                }
            }
        }
    }
}

# ---------------------------------------------------------------------------
# Verification harness (spec 7.1 / 9 M1 deliverables): randomized writers over
# every keyspace write path while a hybrid save runs, then reload-and-diff
# against the fork-instant reference (DEBUG DIGEST taken right before BGSAVE),
# static equivalence against a stock save, CoW comparison, child crash and
# shutdown during a save, and a cluster-mode (per-slot) run.
# ---------------------------------------------------------------------------

set ::snap_module [file normalize tests/modules/datatype.so]

proc snap_hybrid_supported {} { return 1 }

proc snap_wait_save_started {} {
    wait_for_condition 100 50 {[s snap_save_in_progress] == 1} else { fail "hybrid save not started" }
}

proc snap_wait_bgsave_done {} {
    wait_for_condition 300 100 {
        [s rdb_bgsave_in_progress] == 0 && [s snap_save_in_progress] == 0
    } else { fail "bgsave did not finish" }
}

proc snap_seq_list {prefix n} {
    set l {}
    for {set k 0} {$k < $n} {incr k} { lappend l $prefix$k }
    return $l
}
proc snap_seq_pairs {prefix n} {
    set l {}
    for {set k 0} {$k < $n} {incr k} { lappend l $prefix$k v$k }
    return $l
}
proc snap_zpairs {n} {
    set l {}
    for {set k 0} {$k < $n} {incr k} { lappend l $k m$k }
    return $l
}

# Bulk payloads that push a small encoding past its conversion threshold
# (hash/set/zset listpack limit 128 entries, list listpack limit 8 KB).
set ::snap_bulk_hash [snap_seq_pairs g 140]
set ::snap_bulk_set [snap_seq_list q 140]
set ::snap_bulk_zset [snap_zpairs 140]
set ::snap_bulk_list [lrepeat 100 [string repeat y 100]]

# type + DEBUG DIGEST-VALUE of every key of the selected db (pipelined), for a
# readable diff when the whole-keyspace digest differs.
proc snap_key_digests {} {
    set keys [lsort [r keys *]]
    set rd [redis_deferring_client]
    foreach k $keys { $rd type $k; $rd debug digest-value $k; $rd pttl $k }
    set d [dict create]
    foreach k $keys {
        set t [$rd read]; set dg [$rd read]; set ttl [$rd read]
        dict set d $k [list $t $dg [expr {$ttl >= 0 ? "ttl" : "nottl"}]]
    }
    $rd close
    dict set d "#dbsize" [list [r dbsize] [llength $keys] -]
    return $d
}

proc snap_fail_with_diff {ref got what} {
    set diff {}
    foreach k [dict keys $ref] {
        if {![dict exists $got $k]} { lappend diff "$k: missing ([lindex [dict get $ref $k] 0])"; continue }
        if {[dict get $got $k] ne [dict get $ref $k]} { lappend diff "$k: [dict get $ref $k] -> [dict get $got $k]" }
    }
    foreach k [dict keys $got] { if {![dict exists $ref $k]} { lappend diff "$k: extra ([lindex [dict get $got $k] 0])" } }
    fail "$what: [llength $diff] keys differ: [join [lrange $diff 0 9] {; }]"
}

proc snap_rdb_path {} {
    return [file join [lindex [r config get dir] 1] [lindex [r config get dbfilename] 1]]
}

# Base dataset for the harness: every type in both classes, key TTLs and hash
# field TTLs that outlive the test, optionally module keys and two more dbs.
# Deterministic (explicit stream IDs) so two servers produce the same digest.
proc snap_populate_race {multidb module} {
    r flushall
    set big [string repeat B 9000]
    for {set i 0} {$i < 80} {incr i} {
        r set s:$i v$i
        r set n:$i $i
        r hset h:$i f0 a f1 b f2 c
        r sadd st:$i 1 2 3
        r zadd z:$i 1 a 2 b 3 c
        r rpush l:$i a b c
        if {$i % 3 == 0} { r expire s:$i 100000 }
        if {$i % 2 == 0} { r hexpire h:$i 100000 fields 1 f0 }
        if {$i < 20} {
            r set b:$i $big
            r pexpire b:$i 100000000
            r xadd x:$i [expr {$i + 1}]-1 f v
            r xadd x:$i [expr {$i + 1}]-2 f w
            if {$module} { r datatype.set m:$i $i str$i }
        }
        if {$i < 10} {
            r hset hbig:$i {*}[snap_seq_pairs f 600]
            r hexpire hbig:$i 100000 fields 1 f1
            r rpush lbig:$i {*}[lrepeat 2000 [string repeat y 100]]
            r sadd stbig:$i {*}[snap_seq_list m 600]
            r zadd zbig:$i {*}[snap_zpairs 300]
        }
    }
    for {set i 0} {$i < 3000} {incr i} { r set filler:$i [string repeat f 60] }
    if {$multidb} {
        r select 1
        for {set i 0} {$i < 200} {incr i} { r set d1:$i v$i; r hset d1h:$i f v }
        r set d1:big $big
        r select 2
        r rpush d2:l {*}[lrepeat 2000 z]
        r select 9
    }
}

# One random write over the keyspace built by snap_populate_race, drawn from
# the write-path audit: value writes of both classes, class-crossing growth and
# encoding conversions, header-only mutations of SNAP_FORK keys (EXPIRE/PERSIST
# on b:*, hbig:*), deletes, re-adds, RENAME/COPY/MOVE/RESTORE, hash field TTLs,
# streams, module writes, short-lived keys (lazy and active expiry) and reads.
# Returns 1 if the command was accepted; error replies (WRONGTYPE, missing
# source, CROSSSLOT in cluster mode, OOM under eviction) are expected and
# return 0.
proc snap_random_op {multidb module} {
    set i [randomInt 80]
    set j [randomInt 80]
    set k [randomInt 10]
    switch [randomInt 44] {
        0 { set cmd [list set s:$i v[randomInt 1000]] }
        1 { set cmd [list set s:$i [string repeat B 9000]] }
        2 { set cmd [list append s:$i [string repeat a 5000]] }
        3 { set cmd [list setrange s:$i 100 xyz] }
        4 { set cmd [list incr n:$i] }
        5 { set cmd [list getex s:$i ex 1000] }
        6 { set cmd [list getdel s:$i] }
        7 { set cmd [list del s:$i h:$i] }
        8 { set cmd [list unlink l:$i z:$i] }
        9 { set cmd [list expire s:$i 1000] }
        10 { set cmd [list pexpire b:[randomInt 20] 1000000] }
        11 { set cmd [list persist b:[randomInt 20]] }
        12 { set cmd [list rename s:$i s:$j] }
        13 { set cmd [list copy s:$i s:$j replace] }
        14 {
            if {[catch {set dump [r dump s:$i]}] || $dump eq ""} { return 0 }
            set cmd [list restore s:$j 0 $dump replace]
        }
        15 { if {!$multidb} { return 1 }; set cmd [list move s:$i 1] }
        16 { set cmd [list hset h:$i f[randomInt 20] v[randomInt 100]] }
        17 { set cmd [list hset h:$i {*}$::snap_bulk_hash] }
        18 { set cmd [list hdel h:$i f1 f2] }
        19 { set cmd [list hexpire h:$i 1000 fields 1 f0] }
        20 { set cmd [list hpersist h:$i fields 1 f0] }
        21 { set cmd [list hincrby h:$i cnt 1] }
        22 { set cmd [list sadd st:$i [randomInt 100]] }
        23 { set cmd [list sadd st:$i m[randomInt 100]] }
        24 { set cmd [list sadd st:$i {*}$::snap_bulk_set] }
        25 { set cmd [list srem st:$i 1 m1] }
        26 { set cmd [list spop st:$i] }
        27 { set cmd [list zadd z:$i [randomInt 100] m[randomInt 50]] }
        28 { set cmd [list zadd z:$i {*}$::snap_bulk_zset] }
        29 { set cmd [list zpopmin z:$i] }
        30 { set cmd [list rpush l:$i x] }
        31 { set cmd [list rpush l:$i {*}$::snap_bulk_list] }
        32 { set cmd [list ltrim l:$i 0 1] }
        33 { set cmd [list lpop l:$i] }
        34 { set cmd [list lset l:$i 0 first] }
        35 { set cmd [list xadd x:[randomInt 20] * f v] }
        36 { set cmd [list xtrim x:[randomInt 20] maxlen 1] }
        37 { if {!$module} { return 1 }; set cmd [list datatype.set m:[randomInt 20] [randomInt 100] s[randomInt 9]] }
        38 { set cmd [list set exp:$i v px [expr {50 + [randomInt 400]}]] }
        39 { set cmd [list get exp:$i] }
        40 { set cmd [list hset hbig:$k f[randomInt 700] w] }
        41 { set cmd [list rpush lbig:$k tail] }
        42 { set cmd [list hexpire hbig:$k 1000 fields 1 f2] }
        43 { set cmd [list ltrim lbig:$k 0 1] }
    }
    set ::snap_last_op [lrange $cmd 0 1]
    if {[catch {r {*}$cmd} err]} {
        if {$::verbose && ![string match {*WRONGTYPE*} $err] && ![string match {*no such key*} $err]
            && ![string match {*CROSSSLOT*} $err] && ![string match {*OOM*} $err]} {
            puts "snap_random_op: [lrange $cmd 0 1] -> $err"
        }
        return 0
    }
    return 1
}

# One race iteration: reference digest, slow hybrid save, 'ops' random writes
# (optionally under an eviction policy with maxmemory tight enough to evict
# during the save), reload the produced file and compare with the reference.
proc snap_race_iteration {ops multidb module {policy ""}} {
    r config set maxmemory 0
    r config set maxmemory-policy noeviction
    snap_populate_race $multidb $module
    set ref [debug_digest]
    set refkeys [snap_key_digests]
    r config set rdb-key-save-delay 400
    r bgsave
    snap_wait_save_started
    set evicted0 [s evicted_keys]
    if {$policy ne ""} {
        r config set maxmemory-policy $policy
        r config set maxmemory [expr {[s used_memory] - 4096}]   ;# already over: the next writes evict
    }
    set okops 0
    r config set slowlog-log-slower-than 1000
    r slowlog reset
    r config set latency-monitor-threshold 2
    r latency reset
    set t0 [clock milliseconds]
    set raced 0
    set racing 1
    for {set n 0} {$n < $ops} {incr n} {
        incr okops [snap_random_op $multidb $module]
        if {$racing} { incr raced }
        if {$n % 50 == 49 && $racing} { set racing [s snap_save_in_progress] }
    }
    set t1 [clock milliseconds]
    if {$::verbose} {
        foreach e [r slowlog get 8] { puts "  slow: [lindex $e 2] us [lrange [lindex $e 3] 0 2]" }
        puts "  latency latest: [r latency latest]"
        puts "snap_race_iteration: $ops ops in [expr {$t1 - $t0}] ms, $raced raced the save"
    }
    r config set latency-monitor-threshold 0
    r config set slowlog-log-slower-than 10000
    # most of the writes must have raced the scan
    assert {$raced >= $ops / 2}
    r config set maxmemory 0
    r config set rdb-key-save-delay 0
    snap_wait_bgsave_done
    assert_equal ok [s rdb_last_bgsave_status]
    if {$policy ne ""} {
        set evicted [expr {[s evicted_keys] - $evicted0}]
        if {$::verbose} { puts "snap_race_iteration: $policy evicted $evicted keys, $okops/$ops ops accepted" }
        assert {$evicted > 0}
    } else {
        assert {$okops > $ops / 2}
    }
    snap_assert_placement_clean
    after 500   ;# short-PX keys written during the save must be gone before the reload
    r debug reload nosave
    if {[debug_digest] ne $ref} { snap_fail_with_diff $refkeys [snap_key_digests] "snapshot differs from the fork instant" }
    r config set maxmemory-policy noeviction
}

# Populate 'nkeys' strings of 'valsize' bytes, start a slow save, then either
# overwrite every value or EXPIRE every key while it runs. Returns the child's
# reported CoW bytes (works for both the hybrid and the stock save).
proc snap_cow_run {nkeys valsize delay writer} {
    r flushall
    set rd [redis_deferring_client]
    for {set i 0} {$i < $nkeys} {incr i} { $rd set cow:$i [string repeat v $valsize] }
    for {set i 0} {$i < $nkeys} {incr i} { $rd read }
    r config set rdb-key-save-delay $delay
    r bgsave
    wait_for_condition 100 50 {[s rdb_bgsave_in_progress] == 1} else { fail "bgsave not started" }
    if {$writer ne "none"} {
        for {set i 0} {$i < $nkeys} {incr i} {
            if {$writer eq "set"} {
                $rd set cow:$i [string repeat w $valsize]
            } else {
                $rd expire cow:$i 100000
            }
        }
        for {set i 0} {$i < $nkeys} {incr i} { $rd read }
        # (no "still in progress" check: with serialize-before-write every
        # overwritten bucket is emitted by the main thread and skipped by the
        # scan thread, so the save may legitimately finish before the storm)
    }
    r config set rdb-key-save-delay 0
    snap_wait_bgsave_done
    assert_equal ok [s rdb_last_bgsave_status]
    $rd close
    return [s rdb_last_cow_size]
}

start_server {tags {"snapshot harness external:skip"} overrides {bgsave-hybrid yes save "" repl-diskless-sync no}} {
    if {[snap_hybrid_supported]} {
        r module load $::snap_module

        proc snap_populate_static {} {
            snap_populate_race [expr {!$::singledb}] 1
            r function flush
            r function load {#!lua name=snaplib
                redis.register_function('snapf', function(keys, args) return 1 end)}
        }

        test {Hybrid BGSAVE harness: mixed multi-db dataset with functions and module keys reloads identically} {
            snap_populate_static
            set ref [debug_digest]
            set funcs [r function list]
            r bgsave
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            set ::snap_hybrid_rdb [snap_rdb_path]
            set out [exec [file normalize src/redis-check-rdb] $::snap_hybrid_rdb]
            assert_match {*RDB looks OK*} $out
            r debug reload nosave
            assert_equal $ref [debug_digest]
            assert_equal $funcs [r function list]
            assert {[lindex [r httl h:0 fields 1 f0] 0] > 0}
            assert {[lindex [r httl hbig:0 fields 1 f1] 0] > 0}
            assert {[r pttl b:0] > 0}
            assert_equal "5 str5" [r datatype.get m:5]
            if {!$::singledb} {
                r select 2
                assert_equal 2000 [r llen d2:l]
                r select 9
            }
        }

        test {Hybrid BGSAVE harness: hybrid RDB loads into a stock server and equals a stock save of the same dataset} {
            set ref [debug_digest]
            start_server {overrides {bgsave-hybrid no save ""}} {
                r module load $::snap_module
                snap_populate_static
                assert_equal $ref [debug_digest]
                r bgsave
                waitForBgsave r
                r debug reload nosave
                assert_equal $ref [debug_digest]
                file copy -force $::snap_hybrid_rdb [snap_rdb_path]
                r debug reload nosave
                assert_equal $ref [debug_digest]
                assert_equal "5 str5" [r datatype.get m:5]
            }
        }

        set iters [expr {$::accurate ? 100 : 3}]
        test "Hybrid BGSAVE harness: randomized writers over every path race the save ($iters iterations)" {
            for {set it 0} {$it < $iters} {incr it} {
                snap_race_iteration 400 [expr {!$::singledb}] 1
            }
        }

        foreach policy {allkeys-lru allkeys-lfu} {
            test "Hybrid BGSAVE harness: randomized writers under $policy with eviction during the save" {
                snap_race_iteration 400 [expr {!$::singledb}] 1 $policy
            }
        }

        test {Hybrid BGSAVE: writes to keys the scan already passed are applied in place, not cloned} {
            r flushall
            populate_fillers 3000
            set ref [debug_digest]
            set saves0 [s snap_saves]
            r config set rdb-key-save-delay 2000
            r bgsave
            snap_wait_save_started
            wait_for_condition 100 50 {[s snap_save_keys_nofork] > 1500} else { fail "scan did not reach half" }
            for {set i 0} {$i < 3000} {incr i} { r set filler:$i [string repeat h 60] }
            # Under a loaded machine the writes may outlast the scan: the live
            # counter is reset when the save ends, the last-save one keeps it.
            set still_running [s snap_save_in_progress]
            set inplace [expr {$still_running ? [s snap_save_inplace_writes] : [s snap_last_save_inplace_writes]}]
            assert {$inplace > 500}
            assert {$inplace < 3000}
            # the scanned half was written in place, only the rest is retained
            if {$still_running} { assert {[s snap_save_retained_bytes] > 0} }
            r config set rdb-key-save-delay 0
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            assert_equal [expr {$saves0 + 1}] [s snap_saves]
            assert {[s snap_last_save_inplace_writes] >= $inplace}
            assert_equal [string repeat h 60] [r get filler:0]
            r debug reload nosave
            assert_equal $ref [debug_digest]
            snap_assert_placement_clean
        }

        test {Hybrid BGSAVE: writes inside the window go to the side table and drain once the window has moved} {
            r flushall
            populate 10000 sd: 40
            wait_for_condition 100 20 {![string match "*rehashing target*" [r debug htstats 9]]} else { fail "rehash did not finish" }
            set size [snap_main_table_size]
            # two windows over the whole table: the first half is read while
            # the writes below run, the second half is ahead of the walker
            r debug snap-window-buckets [expr {$size / 2}]
            set ref [debug_digest]
            r config set rdb-key-save-delay 1000   ;# 1 ms x 5000 keys: about 5 s per window
            r bgsave
            snap_wait_save_started
            # write half of the keys by name: by hash about half of them sit
            # in the first window (cloned into the side table), the rest in
            # the second (emitted ahead, written in place); the second window
            # keeps the other 2500 or so unwritten keys to walk, so the save
            # outlives the drain by seconds
            for {set i 0} {$i < 5000} {incr i} { r set sd:$i [string repeat d 40] }
            assert_equal 1 [s snap_save_in_progress]
            assert {[s snap_save_side_entries] > 0}
            assert {[s snap_save_main_buckets] > 0}
            # once the walker moves to the second window the side table drains
            # (at the end of a write section or within a cron tick), while the
            # save is still running
            wait_for_condition 400 50 {[s snap_save_side_entries] == 0} else { fail "side table did not drain" }
            assert_equal 1 [s snap_save_in_progress]
            assert_equal 0 [s snap_save_retained_bytes]
            r config set rdb-key-save-delay 0
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            assert_equal [string repeat d 40] [r get sd:0]
            r debug reload nosave
            assert_equal $ref [debug_digest]
            r debug snap-window-buckets 1024
            snap_assert_placement_clean
        }

        test {Hybrid BGSAVE harness: keys with module metadata leave the arena, survive writes during the save and reload with their metadata} {
            r module load [file normalize tests/modules/test_keymeta.so]
            r debug enable-keymeta-runtime-registration 1
            r keymeta.register SNPM 1 KEEPONCOPY:KEEPONRENAME:ALLOWIGNORE:RDBLOAD:RDBSAVE
            r flushall
            # every small-class encoding: raw and embedded strings, listpack
            # hash, listpack-ex hash (field TTL), intset
            r set km:str [string repeat s 200]
            r set km:embstr short
            r hset km:hash f1 v1 f2 v2
            r hset km:hfe f1 v1 f2 v2
            r hexpire km:hfe 100000 fields 1 f1
            assert_equal listpackex [r object encoding km:hfe]
            r sadd km:intset 1 2 3
            set keys {km:str km:embstr km:hash km:hfe km:intset}
            foreach k $keys { r keymeta.set SNPM $k "meta-$k" }
            populate_fillers 2000
            # module metadata is serialized by the child through module
            # callbacks: nothing of these values may stay in the arena
            snap_assert_placement_clean
            set digest [debug_digest]
            r config set rdb-key-save-delay 2000
            r bgsave
            snap_wait_save_started
            # write them while owed: the pre-write emission ships them to the child
            r append km:str more
            r set km:embstr other
            r hset km:hash f3 v3
            r hset km:hfe f3 v3
            r sadd km:intset 4
            r keymeta.set SNPM km:hash changed
            r config set rdb-key-save-delay 0
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            r debug reload nosave
            assert_equal $digest [debug_digest]
            foreach k $keys { assert_equal "meta-$k" [r keymeta.get SNPM $k] }
            snap_assert_placement_clean
            r flushall
        }

        test {Hybrid BGSAVE harness: templated hashes (SNAP_FORK, REF form) survive a hybrid save} {
            r flushall
            r himport prepare fieldset a b c d
            r himport set tmpl:small fieldset 1 2 3 4
            r himport set tmpl:wide fieldset 1 2 3 4
            r hset tmpl:wide a [string repeat x 100]
            assert_encoding template-listpack tmpl:small
            assert_encoding template-array tmpl:wide
            populate_fillers 100
            set ref [debug_digest]
            r bgsave
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            assert_match {*RDB looks OK*} [exec [file normalize src/redis-check-rdb] [snap_rdb_path]]
            r debug reload nosave
            assert_equal $ref [debug_digest]
            assert_equal {1 2 3 4} [r hmget tmpl:small a b c d]
            assert_equal [string repeat x 100] [r hget tmpl:wide a]
            assert_encoding template-listpack tmpl:small
            snap_assert_placement_clean
        }

        test {Hybrid BGSAVE harness: FLUSHALL ASYNC during the save aborts it and the next save works} {
            snap_populate_race 0 1
            r config set rdb-key-save-delay 400
            r bgsave
            snap_wait_save_started
            r flushall async
            assert_equal 0 [r dbsize]
            assert_equal 0 [s snap_save_in_progress]
            snap_wait_bgsave_done
            r config set rdb-key-save-delay 0
            r set after 1
            r bgsave
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            r debug reload nosave
            assert_equal 1 [r dbsize]
        }

        test {Hybrid BGSAVE harness: passing the retained-bytes hard mark aborts the save, clients never wait} {
            snap_populate_race 0 1
            r config set bgsave-hybrid-max-retained-bytes 65536
            r config set rdb-key-save-delay 2000
            r config set latency-monitor-threshold 50
            r latency reset
            set aborts0 [s snap_hard_mark_aborts]
            r bgsave
            snap_wait_save_started
            # overwrite a few thousand small keys ahead of the slow scan thread:
            # each is serialized into the queue for the child (accounted like
            # retained bytes) faster than the thread forwards it
            for {set i 0} {$i < 3000} {incr i} { r set filler:$i [string repeat g 60] }
            snap_wait_bgsave_done
            assert_equal err [s rdb_last_bgsave_status]
            assert_equal [expr {$aborts0 + 1}] [s snap_hard_mark_aborts]
            assert {[s snap_last_save_retained_peak] > 65536}
            assert_equal 0 [s snap_save_retained_bytes]
            verify_log_message 0 "*bgsave-hybrid-max-retained-bytes*" 0
            assert_equal {} [r latency history command]
            r config set latency-monitor-threshold 0
            r config set bgsave-hybrid-max-retained-bytes 256mb
            r config set rdb-key-save-delay 0
            assert_equal [string repeat g 60] [r get filler:2999]
            set ref [debug_digest]
            r bgsave
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            r debug reload nosave
            assert_equal $ref [debug_digest]
        }

        test {Hybrid BGSAVE harness: a stalled child fills the scan thread's queue up to the bound, the thread waits, the save completes} {
            r flushall
            # rdb compression would shrink the repeated values to a few bytes
            # each; the point is volume: 100k records of about 75 bytes, 7 MB
            r config set rdbcompression no
            set rd [redis_deferring_client]
            for {set i 0} {$i < 100000} {incr i} { $rd set q:$i [string repeat q 60] }
            for {set i 0} {$i < 100000} {incr i} { $rd read }
            # a few thousand hashtable hashes: their small KVOBJ messages must
            # not each pin a whole staging buffer in the queue
            for {set h 0} {$h < 3000} {incr h} {
                set args {}
                for {set j 0} {$j < 64} {incr j} { lappend args f$j [string repeat v 96] }
                $rd hset qh:$h {*}$args
            }
            for {set h 0} {$h < 3000} {incr h} { $rd read }
            $rd close
            assert_equal hashtable [r object encoding qh:0]
            set ref [debug_digest]
            set used0 [s used_memory]
            # 256 KB bound; the pipe itself holds 1 MB before the queue grows
            r config set bgsave-hybrid-max-queued-bytes 262144
            r config set rdb-key-save-delay 20
            set aborts0 [s snap_hard_mark_aborts]
            r bgsave
            snap_wait_save_started
            set child [get_child_pid 0]
            exec kill -STOP $child
            # Observe while the child is stopped, assert after it is continued:
            # a failed assertion must never leave the child stopped (the next
            # test's abort could not reach it).
            set reached 0
            for {set n 0} {$n < 200 && !$reached} {incr n} {
                after 50
                if {[s snap_save_queued_bytes] >= 262144} { set reached 1 }
            }
            # the scan thread now waits for the child instead of growing the queue
            set max_queued 0; set pings 0; set still_running 1; set max_used 0
            for {set n 0} {$n < 10} {incr n} {
                after 50
                set q [s snap_save_queued_bytes]
                if {$q > $max_queued} { set max_queued $q }
                set u [s used_memory]
                if {$u > $max_used} { set max_used $u }
                if {[s snap_save_in_progress] != 1} { set still_running 0 }
                if {[r ping] eq "PONG"} { incr pings }
            }
            set aborts1 [s snap_hard_mark_aborts]
            exec kill -CONT $child
            r config set rdb-key-save-delay 0
            assert_equal 1 $reached
            assert {$max_queued <= 262144 + 2 * 81920}
            # what the save allocates while stalled: the queue at its bound plus
            # the pooled staging buffers, not hundreds of MB of pinned buffers
            assert {$max_used - $used0 < 8 * 1024 * 1024}
            assert_equal 1 $still_running
            assert_equal 10 $pings
            assert_equal $aborts0 $aborts1
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            assert {[s snap_last_save_queued_peak] >= 262144}
            assert_equal 0 [s snap_save_queued_bytes]
            r config set bgsave-hybrid-max-queued-bytes 100mb
            r config set rdbcompression yes
            r debug reload nosave
            assert_equal $ref [debug_digest]
        }

        test {Hybrid BGSAVE harness: the queue absorbs scan output while the child serializes big fork-class values} {
            r flushall
            r config set rdbcompression no
            set rd [redis_deferring_client]
            for {set i 0} {$i < 400000} {incr i} { $rd set a:$i [string repeat a 100] }
            for {set i 0} {$i < 400000} {incr i} { $rd read }
            for {set h 0} {$h < 4} {incr h} {
                for {set i 0} {$i < 300000} {incr i 1000} {
                    set args {}
                    for {set j $i} {$j < $i + 1000} {incr j} { lappend args f$j [string repeat v 40] }
                    $rd hset bigh:$h {*}$args
                }
            }
            for {set h 0} {$h < 4} {incr h} { for {set i 0} {$i < 300000} {incr i 1000} { $rd read } }
            $rd close
            assert_equal hashtable [r object encoding bigh:0]
            set ref [debug_digest]
            r config set bgsave-hybrid-max-queued-bytes 128mb
            r bgsave
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            r config set bgsave-hybrid-max-queued-bytes 100mb
            # more than the 1 MB pipe was pending at some point: the scan thread ran ahead of the child
            assert {[s snap_last_save_queued_peak] > 1048576}
            assert_equal 0 [s snap_save_queued_bytes]
            r config set rdbcompression yes
            r debug reload nosave
            assert_equal $ref [debug_digest]
        }

        test {Hybrid BGSAVE harness: child killed mid-save, the parent recovers and the next save works} {
            snap_populate_race 0 1
            r config set rdb-key-save-delay 400
            r bgsave
            snap_wait_save_started
            set child [get_child_pid 0]
            exec kill -9 $child
            for {set n 0} {$n < 200} {incr n} { snap_random_op 0 1 }
            snap_wait_bgsave_done
            assert_equal err [s rdb_last_bgsave_status]
            assert_equal PONG [r ping]
            r config set rdb-key-save-delay 0
            # short-lived keys written during the save must be fully gone (not
            # just logically expired: DEBUG DIGEST and the RDB still see those)
            after 600
            wait_for_condition 50 100 {[r dbsize] == [llength [r keys *]]} else { fail "expired keys linger" }
            set ref [debug_digest]
            set refkeys [snap_key_digests]
            r bgsave
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            r debug reload nosave
            if {[debug_digest] ne $ref} { snap_fail_with_diff $refkeys [snap_key_digests] "save after a killed child differs from the live keyspace" }
            snap_assert_placement_clean
        }

        snap_arena_test {Hybrid BGSAVE harness: child CoW stays flat under heavy writes versus stock BGSAVE} {
            # baseline (no writes) first: the child dirties some pages of its own
            set hy_base_small [snap_cow_run 20000 60 50 none]
            set hy_set [snap_cow_run 20000 60 50 set]
            # SNAP_FORK dataset: 20000 values just over a lowered threshold, then
            # an EXPIRE storm touches every header but no value
            r config set save-small-max-bytes 256
            set hy_base_big [snap_cow_run 20000 1000 50 none]
            set hy_exp [snap_cow_run 20000 1000 50 expire]
            assert {[dict get [snap_placement] class_fork] >= 20000}
            r config set save-small-max-bytes 4096
            start_server {overrides {bgsave-hybrid no save ""}} {
                set st_base_small [snap_cow_run 20000 60 50 none]
                set st_set [snap_cow_run 20000 60 50 set]
                set st_base_big [snap_cow_run 20000 1000 50 none]
                set st_exp [snap_cow_run 20000 1000 50 expire]
            }
            set hy_dset [expr {$hy_set - $hy_base_small}]
            set st_dset [expr {$st_set - $st_base_small}]
            set hy_dexp [expr {$hy_exp - $hy_base_big}]
            set st_dexp [expr {$st_exp - $st_base_big}]
            set ::snap_cow_numbers "overwrite-storm: hybrid $hy_base_small -> $hy_set (+$hy_dset), stock $st_base_small -> $st_set (+$st_dset); expire-storm: hybrid $hy_base_big -> $hy_exp (+$hy_dexp), stock $st_base_big -> $st_exp (+$st_dexp)"
            if {$::verbose} { puts "CoW bytes: $::snap_cow_numbers" }
            assert {$hy_dset * 2 < $st_dset}
            assert {$hy_dexp * 2 < $st_dexp}
        }

        test {Hybrid BGSAVE harness: with the spool on the queue fills first, the overflow goes to an unlinked spool file the child drains in order; no file is left} {
            r flushall
            r config set bgsave-hybrid-spool yes
            r config set rdbcompression no
            populate 200000 sp: 60
            for {set i 0} {$i < 3000} {incr i} { r hset sp:big f$i [string repeat v 40] }
            assert_equal hashtable [r object encoding sp:big]   ;# one fork-class value goes through the pipe
            set digest [debug_digest]
            set dir [lindex [r config get dir] 1]
            r config set bgsave-hybrid-max-queued-bytes 1mb
            r config set rdb-key-save-delay 10
            r bgsave
            snap_wait_save_started
            # a child that keeps up never fills the queue: nothing is spooled
            after 200
            assert_equal 0 [s snap_save_spool_bytes]
            # stop the child: the queue fills to its bound, then the output overflows to the spool
            set child [get_child_pid 0]
            exec kill -STOP $child
            wait_for_condition 100 50 {[s snap_save_spool_bytes] > 1000000} else { exec kill -CONT $child; fail "nothing spooled" }
            set queued [s snap_save_queued_bytes]
            set nofile [llength [glob -nocomplain -directory $dir temp-snap-*]]
            exec kill -CONT $child
            assert {$queued > 500000 && $queued <= 1100000}   ;# at the bound (never past it), not beyond
            assert_equal 0 $nofile                              ;# unlinked at creation: the directory never shows it
            r config set rdb-key-save-delay 0
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            assert {[s snap_last_save_spool_bytes] > 1000000}
            # the footprint (allocated bytes, the child punches holes behind
            # its reads) never exceeds what was written and is gone with the save
            assert {[s snap_last_save_spool_peak] > 0 && [s snap_last_save_spool_peak] <= [s snap_last_save_spool_bytes]}
            assert_equal 0 [s snap_save_spool_bytes]
            r debug reload nosave
            assert_equal $digest [debug_digest]
            r config set rdbcompression yes
            r config set bgsave-hybrid-max-queued-bytes 100mb
            r config set bgsave-hybrid-spool no
        }

        test {Hybrid BGSAVE harness: with the spool off (the default) everything goes through the two pipes and the save reloads equal} {
            assert_equal no [lindex [r config get bgsave-hybrid-spool] 1]
            set digest [debug_digest]
            r bgsave
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            assert_equal 0 [s snap_last_save_spool_bytes]
            r debug reload nosave
            assert_equal $digest [debug_digest]
        }

        test {Hybrid BGSAVE harness: a child killed while draining the spool leaves no spool file and the next save works} {
            r flushall
            r config set bgsave-hybrid-spool yes
            r config set rdbcompression no
            populate 100000 spk: 60
            set dir [lindex [r config get dir] 1]
            r config set bgsave-hybrid-max-queued-bytes 1mb
            r config set rdb-key-save-delay 10
            r bgsave
            snap_wait_save_started
            set child [get_child_pid 0]
            exec kill -STOP $child   ;# force the overflow into the spool
            wait_for_condition 100 50 {[s snap_save_spool_bytes] > 0} else { exec kill -CONT $child; fail "nothing spooled" }
            exec kill -9 $child
            wait_for_condition 100 50 {[s snap_save_in_progress] == 0 && [s rdb_bgsave_in_progress] == 0} else { fail "save did not end" }
            assert_equal 0 [llength [glob -nocomplain -directory $dir temp-snap-*]]
            r config set rdb-key-save-delay 0
            r config set bgsave-hybrid-max-queued-bytes 100mb
            r config set bgsave-hybrid-spool no
            r config set rdbcompression yes
            set digest [debug_digest]
            r bgsave
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            r debug reload nosave
            assert_equal $digest [debug_digest]
        }

        test {Hybrid BGSAVE harness: the queue the save holds for its child is not counted for eviction} {
            r flushall
            r config set rdbcompression no
            r config set maxmemory-policy allkeys-random
            populate 20000 ev: 100
            set digest [debug_digest]
            set ev0 [s evicted_keys]
            r config set bgsave-hybrid-max-queued-bytes 1mb
            r config set rdb-key-save-delay 50
            r bgsave
            snap_wait_save_started
            set child [get_child_pid 0]
            exec kill -STOP $child
            wait_for_condition 100 50 {[s snap_save_queued_bytes] >= 1000000} else { exec kill -CONT $child; fail "queue did not fill" }
            # maxmemory a little above what the dataset and the server need:
            # the queue alone would put used_memory past it
            set queued [s snap_save_queued_bytes]
            r config set maxmemory [expr {[s used_memory] - $queued + 300000}]
            for {set i 0} {$i < 20} {incr i} { r ping }
            set evicted [expr {[s evicted_keys] - $ev0}]
            set size [r dbsize]
            exec kill -CONT $child
            r config set maxmemory 0
            r config set rdb-key-save-delay 0
            assert_equal 0 $evicted
            assert_equal 20000 $size
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            r config set bgsave-hybrid-max-queued-bytes 100mb
            r config set rdbcompression yes
            r debug reload nosave
            assert_equal $digest [debug_digest]
        }

        test {Hybrid BGSAVE harness: a buffer that depends on big values waits for them (debug tag-all), the save completes and reloads equal} {
            r flushall
            r config set rdbcompression no
            r config set bgsave-hybrid-tag-all yes
            populate 60000 tg: 60
            for {set h 0} {$h < 4} {incr h} {
                set args {}
                for {set j 0} {$j < 2000} {incr j} { lappend args f$j [string repeat v 40] }
                r hset tgh:$h {*}$args
            }
            assert_equal hashtable [r object encoding tgh:0]
            set digest [debug_digest]
            r config set bgsave-hybrid-max-queued-bytes 1mb
            r config set rdb-key-save-delay 20
            r bgsave
            snap_wait_save_started
            set child [get_child_pid 0]
            # a stopped child lets the small backlog build past the bound, so
            # that once continued it drains small first and meets buffers whose
            # dependency on the big values is not met yet
            exec kill -STOP $child
            # writes to the big hashes hand them to the child at once (emitted
            # ahead): every later buffer depends on them
            for {set i 0} {$i < 100} {incr i} { r hset tgh:[expr {$i % 4}] new$i v }
            wait_for_condition 100 50 {[s snap_save_queued_bytes] >= 1000000} else { exec kill -CONT $child; fail "queue did not fill" }
            exec kill -CONT $child
            r config set rdb-key-save-delay 0
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            r config set bgsave-hybrid-tag-all no
            r config set bgsave-hybrid-max-queued-bytes 100mb
            r config set rdbcompression yes
            r debug reload nosave
            assert_equal $digest [debug_digest]
        }

        test {Hybrid BGSAVE harness: the bucket bitmaps exist only while a save runs, and only once a slot needs them} {
            snap_populate_race 0 0
            assert_equal 0 [s snap_save_bitmap_bytes]
            r config set rdb-key-save-delay 1000
            r bgsave
            snap_wait_save_started
            # on demand: the freeze allocates nothing; the first write to the
            # owed slot (or the walker's arrival) attaches the bitmap
            r set bitmap:probe 1
            assert {[s snap_save_bitmap_bytes] > 0}
            r config set rdb-key-save-delay 0
            snap_wait_bgsave_done
            assert_equal 0 [s snap_save_bitmap_bytes]
        }

        test {Hybrid BGSAVE harness: ten saves aborted by a killed child, then one that completes, reload equal} {
            snap_populate_race 0 1
            for {set k 0} {$k < 10} {incr k} {
                r config set rdb-key-save-delay 400
                r bgsave
                snap_wait_save_started
                exec kill -9 [get_child_pid 0]
                for {set n 0} {$n < 50} {incr n} { snap_random_op 0 1 }
                snap_wait_bgsave_done
                assert_equal err [s rdb_last_bgsave_status]
                assert_equal 0 [s snap_save_bitmap_bytes]
            }
            r config set rdb-key-save-delay 0
            after 600
            wait_for_condition 50 100 {[r dbsize] == [llength [r keys *]]} else { fail "expired keys linger" }
            set ref [debug_digest]
            set refkeys [snap_key_digests]
            r bgsave
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
            r debug reload nosave
            if {[debug_digest] ne $ref} { snap_fail_with_diff $refkeys [snap_key_digests] "save after ten aborted saves differs from the live keyspace" }
            snap_assert_placement_clean
        }

        test {Hybrid BGSAVE harness: SHUTDOWN NOSAVE during a save exits cleanly} {
            start_server {overrides {bgsave-hybrid yes save "" repl-diskless-sync no}} {
                set pid [srv 0 pid]
                snap_populate_race 0 0
                r config set rdb-key-save-delay 400
                r bgsave
                snap_wait_save_started
                catch {r shutdown nosave}
                wait_for_condition 100 50 {![process_is_alive $pid]} else { fail "server did not exit" }
                assert_equal 0 [count_log_message 0 "crashed by signal"]
            }
        }
    }
}

start_cluster 1 0 {tags {"snapshot cluster external:skip"} overrides {bgsave-hybrid yes save "" repl-diskless-sync no}} {
    if {[snap_hybrid_supported]} {
        test {Hybrid BGSAVE harness (cluster mode): per-slot progress is exposed} {
            snap_populate_race 0 0
            r config set rdb-key-save-delay 400
            r bgsave
            snap_wait_save_started
            assert {[s snap_save_slots_total] > 1}
            r config set rdb-key-save-delay 0
            snap_wait_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
        }

        test {Hybrid BGSAVE harness (cluster mode): randomized writers race the per-slot scan} {
            snap_race_iteration 400 0 0
        }
    }
}

# ---------------------------------------------------------------------------
# Arena fork visibility (TASK-1.22): stock fork children (AOF rewrite, diskless
# sync, module fork) map the SNAP_NOFORK arena while they run, the hybrid child
# never does, and bgsave-hybrid is a runtime switch.
# ---------------------------------------------------------------------------
set ::snap_fork_module [file normalize tests/modules/fork.so]

proc snap_wait_forkable {want} {
    if {![snap_arena_available]} return
    wait_for_condition 100 20 {[s snap_nofork_arena_forkable] == $want} else {
        fail "arena forkable flag did not become $want"
    }
}

# ---- Diskless full sync through the hybrid child (TASK-1.14) ----------------

proc snap_sync_dataset {} {
    r flushall
    set rd [redis_deferring_client]
    for {set i 0} {$i < 20000} {incr i} { $rd set k:$i [string repeat v 40] }
    for {set i 0} {$i < 20000} {incr i} { $rd read }
    for {set h 0} {$h < 200} {incr h} {
        set args {}
        for {set j 0} {$j < 64} {incr j} { lappend args f$j [string repeat w 96] }
        $rd hset bh:$h {*}$args
    }
    for {set h 0} {$h < 200} {incr h} { $rd read }
    $rd close
    assert_equal hashtable [r object encoding bh:0]
    r set big [string repeat x 200000]
}

proc snap_wait_link_up {{level 0}} {
    wait_for_condition 500 100 {[s $level master_link_status] eq {up}} else { fail "replication not established" }
}

foreach rdb_channel {yes no} {
    start_server {tags {"snapshot diskless external:skip"} overrides {bgsave-hybrid yes save "" appendonly no repl-diskless-sync yes repl-diskless-sync-delay 0 repl-timeout 10}} {
        r config set repl-rdb-channel $rdb_channel
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set shape [expr {$rdb_channel eq "yes" ? "replica socket" : "parent process pipe"}]

        test "Hybrid diskless sync (rdb-channel $rdb_channel): converges under write load, one full sync, hybrid path" {
            snap_sync_dataset
            set saves0 [s snap_saves]
            set load1 [start_write_load $master_host $master_port 20]
            set load2 [start_write_load $master_host $master_port 20]
            after 200
            start_server {overrides {save ""}} {
                r replicaof $master_host $master_port
                snap_wait_link_up
                after 500
                stop_write_load $load1
                stop_write_load $load2
                wait_for_ofs_sync $master [srv 0 client]
                assert_equal [$master dbsize] [r dbsize]
                assert_equal [$master debug digest] [r debug digest]
                assert_equal [expr {$saves0 + 1}] [s -1 snap_saves]
                assert_equal 1 [s -1 sync_full]
                assert {[count_log_message -1 "Background RDB transfer started by pid"] >= 1}
                assert {[count_log_message -1 "to $shape (hybrid"] >= 1}
                assert_equal 0 [s -1 rdb_bgsave_in_progress]
                # PSYNC continues from the snapshot offset: no second full sync
                after 500
                assert_equal 1 [s -1 sync_full]
                assert_equal up [s master_link_status]
                r replicaof no one
            }
        }

        test "Hybrid diskless sync (rdb-channel $rdb_channel): a replica's own running hybrid save is aborted by the full-sync load" {
            snap_sync_dataset
            start_server {overrides {save "" bgsave-hybrid yes}} {
                populate 3000 old: 40
                r config set rdb-key-save-delay 2000
                r bgsave
                wait_for_condition 50 50 {[s snap_save_in_progress] == 1} else { fail "hybrid save not started on the replica" }
                r replicaof $master_host $master_port
                snap_wait_link_up
                # the load emptied every db: the save was stopped first, as stock kills its child
                assert_equal 0 [s snap_save_in_progress]
                assert {[count_log_message 0 "Hybrid BGSAVE: scan aborted"] >= 1}
                wait_for_ofs_sync $master [srv 0 client]
                assert_equal [$master dbsize] [r dbsize]
                assert_equal [$master debug digest] [r debug digest]
                assert_equal 0 [r exists old:1]
                r config set rdb-key-save-delay 0
                r replicaof no one
            }
        }

        test "Hybrid diskless sync (rdb-channel $rdb_channel): two replicas share one child" {
            snap_sync_dataset
            set saves0 [s snap_saves]
            $master config set repl-diskless-sync-delay 2
            start_server {overrides {save ""}} {
                start_server {overrides {save ""}} {
                    [srv 0 client] replicaof $master_host $master_port
                    [srv -1 client] replicaof $master_host $master_port
                    snap_wait_link_up 0
                    snap_wait_link_up -1
                    wait_for_ofs_sync $master [srv 0 client]
                    wait_for_ofs_sync $master [srv -1 client]
                    assert_equal [$master debug digest] [[srv 0 client] debug digest]
                    assert_equal [$master debug digest] [[srv -1 client] debug digest]
                    assert_equal [expr {$saves0 + 1}] [s -2 snap_saves]
                    assert_equal 2 [s -2 connected_slaves]
                    [srv 0 client] replicaof no one
                    [srv -1 client] replicaof no one
                }
            }
            $master config set repl-diskless-sync-delay 0
        }

        test "Hybrid diskless sync (rdb-channel $rdb_channel): replica disconnect mid-transfer, the next sync works" {
            snap_sync_dataset
            set saves0 [s snap_saves]
            $master config set rdb-key-save-delay 200
            start_server {overrides {save ""}} {
                r replicaof $master_host $master_port
                wait_for_condition 100 50 {[s -1 snap_save_in_progress] == 1} else { fail "hybrid transfer did not start" }
                # drop the replica's connections while the child is streaming
                $master client kill type replica
                wait_for_condition 200 100 {[s -1 rdb_bgsave_in_progress] == 0} else { fail "transfer did not end after the disconnect" }
                assert_equal PONG [$master ping]
                $master config set rdb-key-save-delay 0
                snap_wait_link_up
                wait_for_ofs_sync $master [srv 0 client]
                assert_equal [$master debug digest] [r debug digest]
                assert {[s -1 snap_saves] >= $saves0 + 2}
                r replicaof no one
            }
        }

        if {$rdb_channel eq "yes"} {
            test "Hybrid diskless sync (rdb-channel yes): a replica that stops reading never blocks the master" {
                snap_sync_dataset
                # a bigger stream so the socket fills while the replica is stopped
                set rd [redis_deferring_client]
                for {set i 0} {$i < 100000} {incr i} { $rd set fill:$i [string repeat z 200] }
                for {set i 0} {$i < 100000} {incr i} { $rd read }
                $rd close
                $master config set rdb-key-save-delay 20
                $master config set repl-timeout 3
                start_server {overrides {save ""}} {
                    set replica_pid [srv 0 pid]
                    r replicaof $master_host $master_port
                    wait_for_condition 100 50 {[s -1 snap_save_in_progress] == 1} else { fail "hybrid transfer did not start" }
                    exec kill -STOP $replica_pid
                    # the master keeps serving while its child waits on the socket
                    set pings 0
                    for {set n 0} {$n < 20} {incr n} {
                        after 100
                        if {[$master ping] eq "PONG"} { incr pings }
                    }
                    # the transfer ends by repl-timeout (or the hard mark), never by a hung main thread
                    set ended [catch {wait_for_condition 300 100 {[s -1 rdb_bgsave_in_progress] == 0} else { error "still in progress after 30 s" }} err]
                    exec kill -CONT $replica_pid
                    $master config set rdb-key-save-delay 0
                    $master config set repl-timeout 10
                    assert_equal 20 $pings
                    if {$ended} { fail "transfer did not end while the replica was stopped: $err" }
                    assert_equal 0 [s -1 snap_save_in_progress]
                    # the replica resyncs once it runs again
                    snap_wait_link_up
                    wait_for_ofs_sync $master [srv 0 client]
                    assert_equal [$master debug digest] [r debug digest]
                    r replicaof no one
                }
            }
        }
    }
}

# ---- Module contract: REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE (TASK-1.25) ----

set ::snap_violate_module [file normalize tests/modules/hybridviolate.so]

start_server {tags {"snapshot modules external:skip"} overrides {bgsave-hybrid yes save "" appendonly no}} {
    test {Hybrid BGSAVE modules: the bundled modules do not block the hybrid path} {
        assert_equal {} [s snap_hybrid_blocked_by]
    }

    test {Hybrid BGSAVE modules: a module with a data type and no opt-in is named and sends saves to the fork child} {
        set loglines [count_log_lines 0]
        r module load $::snap_violate_module
        assert_equal hybridviolate [s snap_hybrid_blocked_by]
        wait_for_log_messages 0 {"*has save callbacks but did not declare REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE*"} $loglines 50 10
        r flushall
        r hv.set hv:1 hello
        r set hv:probe 1
        populate 2000 k: 40
        set saves0 [s snap_saves]
        set loglines [count_log_lines 0]
        r bgsave
        waitForBgsave r
        assert_equal ok [s rdb_last_bgsave_status]
        assert_equal $saves0 [s snap_saves]
        wait_for_log_messages 0 {"*falling back to the fork child, module 'hybridviolate'*"} $loglines 50 10
        # the stock child has the keyspace: the callback's probes succeeded there
        wait_for_log_messages 0 {"*hv_save: OpenKey=key Call=reply*"} $loglines 50 10
        if {[snap_arena_available]} { snap_wait_forkable 0 }
        r debug reload nosave
        assert_equal hello [r hv.get hv:1]
    }

    test {Hybrid BGSAVE modules: once opted in the hybrid path runs and the child refuses keyspace calls} {
        r hv.optin 1
        assert_equal {} [s snap_hybrid_blocked_by]
        set saves0 [s snap_saves]
        set loglines [count_log_lines 0]
        r bgsave
        snap_wait_bgsave_done
        assert_equal ok [s rdb_last_bgsave_status]
        assert_equal [expr {$saves0 + 1}] [s snap_saves]
        wait_for_log_messages 0 {"*called RedisModule_OpenKey inside the hybrid BGSAVE child*"} $loglines 50 10
        wait_for_log_messages 0 {"*called RedisModule_Call inside the hybrid BGSAVE child*"} $loglines 50 10
        wait_for_log_messages 0 {"*hv_save: OpenKey=NULL Call=NULL*"} $loglines 50 10
        r debug reload nosave
        assert_equal hello [r hv.get hv:1]
        assert_equal 2002 [r dbsize]
    }

    test {Hybrid BGSAVE modules: withdrawing the opt-in blocks the hybrid path again} {
        r hv.optin 0
        assert_equal hybridviolate [s snap_hybrid_blocked_by]
        r hv.optin 1
        assert_equal {} [s snap_hybrid_blocked_by]
    }

    test {Hybrid BGSAVE modules: RM_SetLRU and RM_SetLFU on a READ handle go through the write funnel during a save} {
        r flushall
        r set plain v
        populate 3000 k: 40
        set digest [debug_digest]
        r config set rdb-key-save-delay 2000
        r bgsave
        snap_wait_save_started
        # [SetLRU read, SetLRU write, SetLFU read, SetLFU write] under the LRU
        # policy: both handles succeed, the READ one after the funnel emitted or
        # cloned the owed key (a WRITE handle did that at open time)
        r config set maxmemory-policy allkeys-lru
        assert_equal {1 1 0 0} [r hv.setlru plain]
        assert {[r object idletime plain] >= 1}
        r config set maxmemory-policy allkeys-lfu
        assert_equal {0 0 1 1} [r hv.setlru plain]
        r config set maxmemory-policy noeviction
        r config set rdb-key-save-delay 0
        snap_wait_bgsave_done
        assert_equal ok [s rdb_last_bgsave_status]
        r debug reload nosave
        assert_equal $digest [debug_digest]
    }
}

start_server {tags {"snapshot fallback external:skip"} overrides {bgsave-hybrid yes save "" appendonly no repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    r module load $::snap_fork_module

    proc snap_fallback_dataset {} {
        r flushall
        populate 3000 k: 40
        r hset h f1 v1 f2 v2
        r sadd s a b c
        r rpush l 1 2 3
        r zadd z 1 a 2 b
        r set big [string repeat x 20000]
    }

    test {Arena fork visibility: INFO exposes it and it is DONTFORK while the feature is on} {
        assert_match {*snap_nofork_arena_forkable:*} [r info memory]
        if {[snap_arena_available]} { assert_equal 0 [s snap_nofork_arena_forkable] }
    }

    test {BGREWRITEAOF with bgsave-hybrid on: the arena is forkable for the child, DONTFORK after, the AOF reloads} {
        snap_fallback_dataset
        set digest [debug_digest]
        r config set rdb-key-save-delay 300
        r config set appendonly yes   ;# starts the rewrite
        if {[snap_arena_available]} {
            wait_for_condition 100 20 {[s snap_nofork_arena_forkable] == 1} else { fail "arena not forkable for the AOF child" }
        }
        r config set rdb-key-save-delay 0
        waitForBgrewriteaof r
        assert_equal ok [s aof_last_bgrewrite_status]
        snap_wait_forkable 0
        r debug loadaof
        assert_equal $digest [debug_digest]
        r config set appendonly no
    }

    test {Diskless full sync with bgsave-hybrid on: the hybrid child streams the replica, the arena is never mapped} {
        snap_fallback_dataset
        set digest [debug_digest]
        set saves0 [s snap_saves]
        start_server {overrides {bgsave-hybrid yes}} {
            r replicaof [srv -1 host] [srv -1 port]
            wait_for_condition 100 100 {
                [s master_link_status] eq {up}
            } else { fail "replication not established" }
            wait_for_condition 50 100 {
                [r dbsize] == [[srv -1 client] dbsize]
            } else { fail "replica did not receive all keys" }
            assert_equal $digest [debug_digest]
            assert_equal [expr {$saves0 + 1}] [s -1 snap_saves]
            assert {[count_log_message -1 "Background RDB transfer started by pid"] >= 1}
            if {[snap_arena_available]} { assert_equal 0 [s -1 snap_nofork_arena_forkable] }
            r replicaof no one
        }
    }

    test {Module fork with bgsave-hybrid on: the child runs and exits with its code} {
        r fork.create 3 100000
        wait_for_condition 20 100 {
            [r fork.exitcode] != -1
        } else { fail "fork didn't terminate" }
        assert_equal 3 [r fork.exitcode]
        snap_wait_forkable 0
    }

    test {Mutual exclusion: a hybrid save keeps the arena DONTFORK, a rewrite asked meanwhile is scheduled and runs after} {
        snap_fallback_dataset
        r config set rdb-key-save-delay 1000
        r bgsave
        snap_wait_save_started
        assert_match {*scheduled*} [r bgrewriteaof]
        assert_equal 1 [s aof_rewrite_scheduled]
        if {[snap_arena_available]} { assert_equal 0 [s snap_nofork_arena_forkable] }
        r config set rdb-key-save-delay 0
        snap_wait_bgsave_done
        assert_equal ok [s rdb_last_bgsave_status]
        wait_for_condition 200 50 {
            [s aof_rewrite_scheduled] == 0 && [s aof_rewrite_in_progress] == 0
        } else { fail "scheduled rewrite did not run" }
        assert_equal ok [s aof_last_bgrewrite_status]
        snap_wait_forkable 0
    }

    test {Mutual exclusion: BGSAVE during a fallback child is scheduled, the arena stays forkable until that child exits} {
        snap_fallback_dataset
        r config set rdb-key-save-delay 1000
        r bgrewriteaof
        wait_for_condition 100 20 {[s aof_rewrite_in_progress] == 1} else { fail "rewrite did not start" }
        if {[snap_arena_available]} { assert_equal 1 [s snap_nofork_arena_forkable] }
        set saves [s snap_saves]
        assert_match {*scheduled*} [r bgsave schedule]
        assert_equal 0 [s snap_save_in_progress]
        if {[snap_arena_available]} { assert_equal 1 [s snap_nofork_arena_forkable] }
        r config set rdb-key-save-delay 0
        waitForBgrewriteaof r
        wait_for_condition 200 50 {
            [s snap_saves] == $saves + 1 && [s rdb_bgsave_in_progress] == 0 && [s snap_save_in_progress] == 0
        } else { fail "scheduled save did not run" }
        assert_equal ok [s rdb_last_bgsave_status]   ;# snap_saves moved: the scheduled save took the hybrid path
        snap_wait_forkable 0
    }

    test {CONFIG SET bgsave-hybrid is refused while a save runs} {
        r config set rdb-key-save-delay 1000
        r bgsave
        snap_wait_save_started
        assert_error {*cannot change*} {r config set bgsave-hybrid no}
        assert_equal yes [lindex [r config get bgsave-hybrid] 1]
        r config set rdb-key-save-delay 0
        snap_wait_bgsave_done
    }

    test {Runtime switch: off takes the stock save path with a forkable arena, on is hybrid again, both reload} {
        snap_fallback_dataset
        set digest [debug_digest]
        r config set bgsave-hybrid no
        snap_wait_forkable 1
        set saves [s snap_saves]
        r bgsave
        waitForBgsave r
        assert_equal ok [s rdb_last_bgsave_status]
        assert_equal $saves [s snap_saves]          ;# stock path, not a hybrid save
        assert_equal 0 [s snap_save_in_progress]
        r debug reload
        assert_equal $digest [debug_digest]
        populate 500 off: 40                        ;# allocated outside the arena while off
        set digest2 [debug_digest]
        r config set bgsave-hybrid yes
        snap_wait_forkable 0
        # keys created while the feature was off keep their stock placement
        # (header outside the arena) until rewritten: expected drift, not a
        # violation, and the hybrid save below must handle them.
        if {[snap_arena_available]} { assert {[dict get [snap_placement] headers_outside] > 0} }
        r bgsave
        snap_wait_bgsave_done
        assert_equal ok [s rdb_last_bgsave_status]
        assert_equal [expr {$saves + 1}] [s snap_saves]
        r debug reload                              ;# re-creates every key: placement is clean again
        assert_equal $digest2 [debug_digest]
        snap_assert_placement_clean
    }

    test {Runtime switch: the arena is forkable from startup when the feature starts off} {
        start_server {overrides {bgsave-hybrid no save ""}} {
            if {[snap_arena_available]} {
                assert_equal 1 [s snap_nofork_arena_forkable]
                r config set bgsave-hybrid yes
                assert_equal 0 [s snap_nofork_arena_forkable]
            }
            populate 100 k: 40
            set d [debug_digest]
            r bgsave
            snap_wait_bgsave_done
            r debug reload
            assert_equal $d [debug_digest]
        }
    }

    r module unload fork
}
