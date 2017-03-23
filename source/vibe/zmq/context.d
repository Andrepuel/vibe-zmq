module vibe.zmq.context;

import core.thread : Thread;
import vibe.zmq.zmq;
import vibe.zmq.inner;
import vibe.zmq.rpc;
import vibe.zmq.framelist;
//import core.thread : Thread;
import std.format : format;

enum Type : ubyte {
    create,
    destroy,
    connect,
    bind,
    send,
    receive,
    setsockopt,
    getsockopt,
    term
};

public struct ZContext {
    private enum master_addr = "inproc://master_loop";
    private enum creating_master_addr = "inproc://creating_master";
    private void* ctx;
    void* master_conn;
    Thread worker_thread;

    @disable this(this);
    this(int) {
        ctx = zmq_ctx_new();
        assert(ctx !is null);

        create_thread();
        create_master_conn();
    }

    ~this() {
        if (ctx is null) return;

        master_conn.zmq_send_t(Type.term, 0);
        Type r = master_conn.zmq_recv_t!Type;
        assert(r == Type.term);
        worker_thread.join();
        zmq_close(master_conn);

        zmq_ctx_term(ctx);
        ctx = null;
    }

    private void create_master_conn() {
        master_conn = zmq_socket(ctx, ZMQ_REQ);
        assert(master_conn !is null);
        int rc = master_conn.zmq_connect(master_addr);
        assert(rc != -1);
    }

    private void create_thread() {
        import vibe.core.core;

        void* ready_conn = zmq_socket(ctx, ZMQ_PULL);
        assert(ready_conn !is null);
        scope(exit) zmq_close(ready_conn);
        int rc = ready_conn.zmq_bind(creating_master_addr);
        assert(rc != -1);

        worker_thread = new Thread(() => loop(ctx));
        worker_thread.start();

        zmq_recv_t!ubyte(ready_conn);
    }

    private static void loop(void* ctx) {
        try {
            loop2(cast(void*)ctx);
        } catch(Throwable e) {
            import std.stdio;
            import core.stdc.stdlib;
            stderr.writeln(e);
            abort();
        }
    }

    private static void loop2(void* ctx) {
        void* master = zmq_socket(ctx, ZMQ_REP);
        assert(master !is null);
        scope(exit) zmq_close(master);
        int rc = zmq_bind(master, master_addr);
        assert(rc != -1);

        {
            void* signal_ready = zmq_socket(ctx, ZMQ_PUSH);
            assert(signal_ready !is null);
            scope(exit) zmq_close(signal_ready);
            signal_ready.zmq_connect(creating_master_addr);
            zmq_send_t!ubyte(signal_ready, 0, 0);
        }

        ZSocketInner*[] sockets;
        zmq_pollitem_t[] polls;
        polls.length = 1;
        polls[0].socket = master;
        polls[0].events = ZMQ_POLLIN;

        while (true) {
            polls.length = 1;
            ZSocketInner*[] back;
            back.length = 0;

            foreach(socket; sockets) {
                zmq_pollitem_t item;
                item.events = socket.cantMask();
                if (item.events == 0) continue;
                item.socket = socket.socket;
                polls ~= item;
                back ~= socket;
            }

            rc = zsig!zmq_poll(polls.ptr, cast(int)polls.length, -1);
            assert(rc != -1, "Got errorno %s".format(zmq_errno));

            foreach(i, poll; polls[1..$]) {
                if (poll.revents & ZMQ_POLLIN) {
                    back[i].setCan(ZSocketInner.Rw.READ, true);
                }
                if (poll.revents & ZMQ_POLLOUT) {
                    back[i].setCan(ZSocketInner.Rw.WRITE, true);
                }
            }

            if (polls[0].revents & ZMQ_POLLIN) {
                Type type;
                zsig!zmq_recv(master, &type, type.sizeof, 0);

                switch(type) {
                    case Type.create:
                        master.runRpc(
                            (int type) {
                                sockets ~= new ZSocketInner(ctx, type);

                                ZFrameList r;
                                r.append(sockets[$-1]);
                                return r;
                            }
                        );
                    break;
                    case Type.destroy:
                        master.runRpc(
                            (ZSocketInner* socket) {
                                import std.algorithm;
                                auto idx = sockets.countUntil(socket);
                                assert(idx >= 0);
                                .destroy(*socket);
                                sockets = sockets.remove!(SwapStrategy.unstable)(idx);
                                ZFrameList r;
                                r.append(0);
                                return r;
                            }
                        );
                    break;
                    case Type.connect:
                        master.runRpc(
                            (ZSocketInner* socket, const(char)[] addr) {
                                import std.string;
                                int rc = zmq_connect(socket.socket, addr.toStringz);
                                assert(rc != -1);

                                ZFrameList r;
                                r.append(0);
                                return r;
                            }
                        );
                    break;
                    case Type.bind:
                        master.runRpc(
                            (ZSocketInner* socket, const(char)[] addr) {
                                import std.string;
                                int rc = zmq_bind(socket.socket, addr.toStringz);
                                assert(rc != -1);

                                ZFrameList r;
                                r.append(0);
                                return r;
                            }
                        );
                    break;
                    case Type.send:
                        master.runRpc(
                            (ZSocketInner* socket, ZFrameList msgs) {
                                socket.setCan(ZSocketInner.Rw.WRITE, false);
                                msgs.sendAll(socket.socket);

                                ZFrameList r;
                                r.append(0);
                                return r;
                            }
                        );
                    break;
                    case Type.receive:
                        master.runRpc(
                            (ZSocketInner* socket) {
                                ZFrameList r;
                                socket.setCan(ZSocketInner.Rw.READ, false);
                                r.recvAll(socket.socket);
                                return r;
                            }
                        );
                    break;
                    case Type.setsockopt:
                        master.runRpc(
                            (ZSocketInner* socket, int opt, ZFrameList value) {
                                assert(value.length == 1);
                                int rc = zmq_setsockopt(socket.socket, opt, value[0].ptr, value[0].length);
                                assert(rc != -1);

                                ZFrameList r;
                                r.append(0);
                                return r;
                            }
                        );
                    break;
                    case Type.getsockopt:
                        master.runRpc(
                            (ZSocketInner* socket, int opt) {
                                char[1024] buf;
                                size_t len = buf.length;
                                int rc = zmq_getsockopt(socket.socket, opt, buf.ptr, &len);
                                assert(rc != -1);

                                ZFrameList r;
                                r.append(buf[0..len]);
                                return r;
                            }
                        );
                    break;
                    case Type.term:
                        assert(sockets.length == 0);
                        master.zmq_send_t(Type.term, 0);
                    return;
                    default: assert(false);
                }
            }
        }

    }
};
