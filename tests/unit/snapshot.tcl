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

        test {Hybrid BGSAVE: writes during the save land in the overlay, snapshot keeps the fork instant} {
            populate_all_types
            populate_fillers 3000
            set digest [debug_digest]
            r config set rdb-key-save-delay 300
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

        test {Hybrid BGSAVE: FLUSHDB and SWAPDB during the save abort it} {
            populate_fillers 3000
            r config set rdb-key-save-delay 300
            r bgsave
            wait_for_condition 50 50 {[s snap_save_in_progress] == 1} else { fail "hybrid save not started" }
            r flushdb
            assert_equal 0 [s snap_save_in_progress]
            wait_hybrid_bgsave_done
            populate_fillers 3000
            r bgsave
            wait_for_condition 50 50 {[s snap_save_in_progress] == 1} else { fail "hybrid save not started" }
            r swapdb 9 10
            assert_equal 0 [s snap_save_in_progress]
            wait_hybrid_bgsave_done
            r swapdb 9 10
            r config set rdb-key-save-delay 0
            assert_equal 3000 [r dbsize]
            r bgsave
            wait_hybrid_bgsave_done
            assert_equal ok [s rdb_last_bgsave_status]
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
        assert_equal 1 [s rdb_bgsave_in_progress]
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
            # overwrite a few thousand small keys: each keeps its original alive
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
