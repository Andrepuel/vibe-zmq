module vibe.zmq.rpc;

import vibe.zmq.zmq;
import vibe.zmq.framelist;

ZFrameList call(alias each, Args...)(ZFrameList msgs, ZFrameList delegate(Args) total) {
    import std.algorithm;
    import std.range;
    import std.format;

    mixin("return total(" ~ 0.iota(Args.length).map!(x => "each!(Args[%d])(%d, msgs)".format(x, x)).join(", ") ~ ");");
}

T convertMsgToType(T)(size_t pos, ref ZFrameList msgs)
if(!is(T == ZFrameList) && !is(T == const(char)[]))
{
    return msgs.get!T(pos);
}

const(char)[] convertMsgToType(T)(size_t pos, ref ZFrameList msgs)
if(is(T == const(char)[]))
{
    return msgs[pos];
}

ZFrameList convertMsgToType(T)(size_t pos, ref ZFrameList msgs)
if(is(T == ZFrameList))
{
    return msgs.slice(pos);
}

void runRpc(Args...)(void* master, ZFrameList delegate(Args) ans) {
    import std.algorithm;
    import std.range;

    scope(failure) assert(false);

    static assert(Args.length > 0);
    foreach(k, v; Args) {
        static if(is(v == ZFrameList)) {
            static assert(k == Args.length - 1, "ZFrameList may be only the last element");
        }
    }
    
    ZFrameList args;
    assert(zmq_getsockopt_t!int(master, ZMQ_RCVMORE));
    args.recvAll(master);
    static if(is(Args[$-1] == ZFrameList)) {
        assert(args.length >= Args.length - 1);
    } else {
        assert(args.length == Args.length);
    }

    ZFrameList result = call!convertMsgToType(args.move, ans);
    result.sendAll(master);
}
