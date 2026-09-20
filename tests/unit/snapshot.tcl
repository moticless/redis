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

start_server {tags {"snapshot placement"} overrides {bgsave-hybrid yes}} {
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

        # NOTE: replication full sync with bgsave-hybrid requires the hybrid save
        # path (TASK-1.9); a stock fork child cannot read DONTFORK headers.
    }
}
