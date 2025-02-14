package com.wiqer.redis.command.impl;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.*;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.SimpleString;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Type extends AbstractCore<RedisData> implements Command {

    private BytesWrapper key;

    private static final Map<Class<?>, SimpleString> map = new HashMap<>();

    static {
        map.put(null, new SimpleString("none"));
        map.put(RedisString.class, new SimpleString("string"));
        map.put(RedisList.class, new SimpleString("list"));
        map.put(RedisSet.class, new SimpleString("set"));
        map.put(RedisHash.class, new SimpleString("hash"));
        map.put(RedisZset.class, new SimpleString("zset"));
    }

    @Override
    public String type() {
        return CommonCommandType.type.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
        key = ((BulkString) array.get(1)).getContent();
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        RedisData redisData = get(key);
        ctx.writeAndFlush(map.get(redisData.getClass()));
    }
}
