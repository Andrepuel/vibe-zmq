module vibe.zmq.framelist;

import vibe.zmq.zmq;
import std.exception : enforce;

public struct ZFrameList {
    zmq_msg_t[] list;

    static ZFrameList concat(ref ZFrameList a, ref ZFrameList b) {
        ZFrameList r;
        r.list = a.list;
        a.list.length = 0;
        r.list ~= b.list;
        b.list.length = 0;
        return r;
    }

    void append(ref zmq_msg_t msg) {
        list.length += 1;
        int rc = zmq_msg_init(&list[$-1]);
        assert(rc != -1);
        rc = zmq_msg_copy(&list[$-1], &msg);
        assert(rc != -1);
    }

    void append(const(void)[] data) {
        list.length += 1;
        int rc = zmq_msg_init_size(&list[$-1], data.length);
        assert(rc != -1);
        (cast(char*)zmq_msg_data(&list[$-1]))[0..data.length] = cast(const(char)[]) data;
    }

    void append(T)(const(T) t) {
        append((&t)[0..1]);
    }

    @disable this(this);

    ~this() {
        foreach(ref l; list) {
            zmq_msg_close(&l);
        }
        list.length = 0;
    }

    const(char)[] opIndex(size_t msg) {
        auto size = zmq_msg_size(&list[msg]);
        return (cast(char*)zmq_msg_data(&list[msg]))[0..size];
    }

    ref T get(T)(size_t pos) {
        auto d = this[pos];
        enforce(d.length == T.sizeof);
        return (cast(T[])d)[0];
    }

    ZFrameList clone() {
        ZFrameList r;
        foreach(ref l; list) {
            r.append(l);
        }
        return r;
    }

    void sendAll(void* socket) {
        while (list.length > 0) {
            int rc = zsig!zmq_msg_send(&list[0], socket, list.length > 1 ? ZMQ_SNDMORE : 0);
            assert(rc != -1); 
            list = list[1..$];
        }
    }

    void recvAll(void* socket) {
        do {
            list.length += 1;
            int rc = zmq_msg_init(&list[$-1]);
            assert(rc != -1);
            rc = zsig!zmq_msg_recv(&list[$-1], socket, 0);
            assert(rc != -1);
        } while(zmq_getsockopt_t!int(socket, ZMQ_RCVMORE));
    }

    size_t length() const {
        return list.length;
    }

    ZFrameList slice(size_t start) {
        ZFrameList r;
        r.list = list[start..$];
        list = list[0..start];
        return r;
    }
};
