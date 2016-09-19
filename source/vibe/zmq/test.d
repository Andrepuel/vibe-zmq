module vibe.zmq.test;

void testTask(void function() t) {
    import vibe.core.core;

    Throwable fail;
    runTask(() {
        try {
            t();
        } catch(Throwable e) {
            fail = e;
        }
        exitEventLoop();
    });
    runEventLoop();
    if (fail) throw fail;
}
