module vibe.zmq.inner;

import vibe.zmq.zmq;
import std.exception : enforce;

struct ZSocketInner {
    static import vibe.core.sync;
    static import core.sync.condition;
    static import core.sync.mutex;
    alias Mutex = core.sync.mutex.Mutex;
    alias Condition = vibe.core.sync.InterruptibleTaskCondition;

    void* socket;

    enum Rw {
        READ, WRITE
    }
    
    bool[2] can;
    Mutex canMutex;
    Condition canChanged;

    @disable this(this);
    this(void* ctx, int type) {
        socket = zmq_socket(ctx, type);
        enforce(socket !is null);
        canMutex = new Mutex;
        canChanged = new Condition(canMutex);
    }

    ~this() {
        if (socket is null) return;
        zmq_close(socket);
    }

    void setCan(Rw rw, bool set) {
        synchronized(canMutex) {
            if (can[rw] == set) return;
            can[rw] = set;
            canChanged.notifyAll();
        }
    }

    short cantMask() {
        short r;
        synchronized(canMutex) {
            if (!can[Rw.READ]) {
                r |= ZMQ_POLLIN;
            }
            if (!can[Rw.WRITE]) {
                r |= ZMQ_POLLOUT;
            }
        }
        return r;
    }

    void needs(Rw rw) {
        synchronized(canMutex) {
            while (!can[rw]) {
                canChanged.wait();
            }
        }
    }
};
