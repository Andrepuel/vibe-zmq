module vibe.zmq.socket;

import vibe.zmq.zmq;
import vibe.zmq.inner;
import vibe.zmq.context;
import vibe.zmq.framelist;

import std.exception : enforce;

public struct ZSocket {
    @disable this(this);

    ZSocketInner* inner;
    ZContext* ctx;

    @property private void* master_conn() {
        assert(ctx.master_conn !is null);
        return ctx.master_conn;
    }

    this(ref ZContext ctx, int type) {
        this.ctx = &ctx;

        zmq_send_t(master_conn, Type.create, ZMQ_SNDMORE);
        zmq_send_t(master_conn, type, 0);
        inner = zmq_recv_t!(ZSocketInner*)(master_conn);
    }

    ~this() {
        if (inner is null) return;
        zmq_send_t(master_conn, Type.destroy, ZMQ_SNDMORE);
        zmq_send_t(master_conn, inner, 0);
        int r = zmq_recv_t!int(master_conn);
        assert(r == 0);
        inner = null;
    }

    void bind(const(char)[] addr) {
        assert(inner !is null);

        zmq_send_t(master_conn, Type.bind, ZMQ_SNDMORE);
        zmq_send_t(master_conn, inner, ZMQ_SNDMORE);
        int rc = zsig!zmq_send(master_conn, addr.ptr, addr.length, 0);
        assert(rc != -1);
        int r = zmq_recv_t!int(master_conn);
        assert(r == 0);
    }

    void connect(const(char)[] addr) {
        assert(inner !is null);

        zmq_send_t(master_conn, Type.connect, ZMQ_SNDMORE);
        zmq_send_t(master_conn, inner, ZMQ_SNDMORE);
        int rc = zsig!zmq_send(master_conn, addr.ptr, addr.length, 0);
        assert(rc != -1);
        int r = zmq_recv_t!int(master_conn);
        assert(r == 0);
    }

    void send(ZFrameList frames) {
        assert(inner !is null);
        assert(frames.list.length > 0);

        inner.needs(ZSocketInner.Rw.WRITE);
        zmq_send_t(master_conn, Type.send, ZMQ_SNDMORE);
        zmq_send_t(master_conn, inner, ZMQ_SNDMORE);
        frames.sendAll(master_conn);

        int r = zmq_recv_t!int(master_conn);
        assert(r == 0);
    }

    ZFrameList recv() {
        assert(inner !is null);

        inner.needs(ZSocketInner.Rw.READ);
        zmq_send_t(master_conn, Type.receive, ZMQ_SNDMORE);
        zmq_send_t(master_conn, inner, 0);
        ZFrameList r;
        r.recvAll(master_conn);
        return r;
    }

    void getsockopt(int opt, void[] value) {
        assert(inner !is null);

        zmq_send_t(master_conn, Type.getsockopt, ZMQ_SNDMORE);
        zmq_send_t(master_conn, inner, ZMQ_SNDMORE);
        zmq_send_t(master_conn, opt, 0);

        ZFrameList r;
        r.recvAll(master_conn);
        assert(r.length == 1);
        assert(r[0].length == value.length);
        (cast(char[])value)[] = r[0];
    }

    T getsockopt(T)(int opt) {
        T[1] r;
        getsockopt(opt, r[]);
        return r[0];
    }

    void setsockopt(int opt, const(void)[] value) {
        assert(inner !is null);

        zmq_send_t(master_conn, Type.setsockopt, ZMQ_SNDMORE);
        zmq_send_t(master_conn, inner, ZMQ_SNDMORE);
        zmq_send_t(master_conn, opt, ZMQ_SNDMORE);
        int rc = zsig!zmq_send(master_conn, value.ptr, value.length, 0);
        assert(rc != -1);

        int r = zmq_recv_t!int(master_conn);
        assert(r == 0);
    }
};

version (unittest) {
    import vibe.zmq.test;
}

unittest {
    import vibe.core.core;
    import std.algorithm : move;

    testTask(() {
        ZContext ctx = ZContext(0);
        runTask(() {
            ZSocket a = ZSocket(ctx, ZMQ_REP);
            a.bind("tcp://*:9999");
            a.send(a.recv());
        });

        ZSocket b = ZSocket(ctx, ZMQ_REQ);
        b.connect("tcp://127.0.0.1:9999");
        ZFrameList sending;
        sending.append!int(321);
        b.send(sending.move);
        auto received = b.recv();
        enforce(received.length == 1);
        enforce(received.get!int(0) == 321);
    });
}
