module vibe.zmq.zmq;
public import deimos.zmq.zmq;
import std.format : format;

int zsig(alias f, Args...)(Args args) {
    import core.stdc.errno : EINTR;
    int rc;
    do {
        rc = f(args);
    } while(rc == -1 && zmq_errno() == EINTR);
    return rc;
}

void zmq_send_t(T)(void* socket, in T t, int flags) {
    int rc = zsig!zmq_send(socket, &t, T.sizeof, flags);
    assert(rc != -1);
}

T zmq_getsockopt_t(T)(void* socket, int opt) {
    T r;
    size_t len = T.sizeof;
    int rc = zmq_getsockopt(socket, opt, &r, &len);
    assert(rc != -1);
    assert(len == T.sizeof);
    return r;
}

T zmq_recv_t(T)(void* socket) {
    zmq_msg_t msg;
    int rc = zmq_msg_init(&msg);
    assert(rc != -1);
    rc = zsig!zmq_msg_recv(&msg, socket, 0);
    assert(rc != -1, "Got errorno %s".format(zmq_errno));
    scope(exit) zmq_msg_close(&msg);
    assert(zmq_getsockopt_t!int(socket, ZMQ_RCVMORE) == 0);
    assert(zmq_msg_size(&msg) == T.sizeof);
    return (cast(T*)zmq_msg_data(&msg))[0];
}
