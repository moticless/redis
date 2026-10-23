set testmodule [file normalize tests/modules/fork.so]

start_server {tags {"modules external:skip"}} {
    r module load $testmodule

    test {Module fork} {
        # the argument to fork.create is the exitcode on termination
        # the second argument to fork.create is passed to usleep
        r fork.create 3 100000 ;# 100ms
        wait_for_condition 20 100 {
            [r fork.exitcode] != -1
        } else {
            fail "fork didn't terminate"
        }
        r fork.exitcode
    } {3}

    test {Module fork kill} {
        # use a longer time to avoid the child exiting before being killed
        r fork.create 3 100000000 ;# 100s
        wait_for_condition 20 100 {
            [count_log_message 0 "fork child started"] == 2
        } else {
            fail "fork didn't start"
        }

        # module fork twice
        assert_error {Fork failed} {r fork.create 0 1}
        assert {[count_log_message 0 "Can't fork for module: File exists"] eq "1"}

        r fork.kill

        assert {[count_log_message 0 "Received SIGUSR1 in child"] eq "1"}
        # check that it wasn't printed again (the print belong to the previous test)
        assert {[count_log_message 0 "fork child exiting"] eq "1"}
    }

    test {Module fork without keyspace: the child runs on module memory and its keyspace calls are refused} {
        r set nk:key v
        set loglines [count_log_lines 0]
        r fork.nokeyspace nk:key 100000 ;# 100ms
        wait_for_condition 20 100 {
            [r fork.exitcode] != -1
        } else {
            fail "fork didn't terminate"
        }
        # 0: KeyExists did not see the key, OpenKey and Call were refused with
        # EPERM, the module's own buffer was readable
        assert_equal 0 [r fork.exitcode]
        foreach api {RedisModule_KeyExists RedisModule_OpenKey RedisModule_Call} {
            wait_for_log_messages 0 [list "*called $api inside a module fork child created with REDISMODULE_FORK_NO_KEYSPACE*"] $loglines 50 10
        }
        assert_equal v [r get nk:key]
    }

    test {Module fork without keyspace: unknown flags are rejected and no child is created} {
        assert_equal EINVAL [r fork.badflags]
        assert_equal 0 [s module_fork_in_progress]
    }

    test {Module fork without keyspace: it takes the single child slot like any fork} {
        set loglines [count_log_lines 0]
        r fork.nokeyspace nk:key 100000000 ;# 100s
        wait_for_log_messages 0 {"*nokeyspace child started*"} $loglines 50 100
        assert_equal 1 [s module_fork_in_progress]
        assert_error {*File exists*} {r fork.nokeyspace nk:key 1}
        assert_error {Fork failed} {r fork.create 0 1}
        r fork.kill
        wait_for_condition 50 100 {
            [s module_fork_in_progress] == 0
        } else {
            fail "killed child not reaped"
        }
    }

    test "Unload the module - fork" {
        assert_equal {OK} [r module unload fork]
    }
}
