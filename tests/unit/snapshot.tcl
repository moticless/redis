start_server {tags {"snapshot"}} {
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
