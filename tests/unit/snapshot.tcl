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

start_server {tags {"snapshot placement"} overrides {bgsave-hybrid yes save "" repl-diskless-sync no}} {
    if {[string match {*jemalloc*} [s mem_allocator]] && $::tcl_platform(os) eq "Linux"} {
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

        test {SNAP placement: all types placed, no violations} {
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

        test {SNAP placement: encoding conversions keep placement consistent} {
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

        test {SNAP placement: string growth past the threshold relocates the value} {
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

        test {SNAP placement: DEBUG RELOAD, DUMP/RESTORE and RENAME re-place values} {
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

        test {SNAP placement: OBJECT ENCODING, MEMORY USAGE, DEBUG OBJECT unaffected} {
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
            assert_equal 0 [dict get [snap_placement] violations]
            assert_equal 0 [dict get [snap_placement] headers_outside]
        }

        test {Hybrid BGSAVE: INFO exposes progress while the save runs} {
            populate_fillers 3000
            r config set rdb-key-save-delay 300
            r bgsave
            wait_for_condition 50 50 {[s snap_save_in_progress] == 1} else { fail "hybrid save not started" }
            assert {[s snap_save_slots_total] >= 1}
            r config set rdb-key-save-delay 0
            wait_hybrid_bgsave_done
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
            set st [snap_placement]
            assert_equal 0 [dict get $st violations]
            assert_equal 0 [dict get $st headers_outside]
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
            assert_equal 0 [dict get [snap_placement] violations]
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
                set st [snap_placement]
                assert_equal 0 [dict get $st violations]
                assert_equal 0 [dict get $st headers_outside]
                assert_equal 10 [dict get $st class_nofork]
                assert_equal 6 [dict get $st class_fork]
            }
        }
    }
}
