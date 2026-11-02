redis_folder(core
  SERVER
    acl.c bless.c blocked.c chk.c client_comp.c commands.c config.c connection.c
    db.c debug.c defrag.c estore.c eventnotifier.c evict.c expire.c hotkeys.c
    iothread.c keymeta.c latency.c lazyfree.c logreqres.c memory_prefetch.c multi.c
    networking.c notify.c object.c pubsub.c server.c slowlog.c socket.c sort.c
    threads_mngr.c timeout.c tls.c tracking.c unix.c
  COMMON
    release.c)
