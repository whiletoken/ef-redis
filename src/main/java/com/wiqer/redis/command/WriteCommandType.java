package com.wiqer.redis.command;

import com.wiqer.redis.command.impl.*;
import com.wiqer.redis.command.impl.hash.Hdel;
import com.wiqer.redis.command.impl.hash.Hset;
import com.wiqer.redis.command.impl.list.Lpush;
import com.wiqer.redis.command.impl.list.Lrem;
import com.wiqer.redis.command.impl.set.Sadd;
import com.wiqer.redis.command.impl.set.Srem;
import com.wiqer.redis.command.impl.string.*;
import com.wiqer.redis.command.impl.zset.Zadd;
import com.wiqer.redis.command.impl.zset.Zrem;
import lombok.Getter;

import java.util.function.Supplier;

@Getter
public enum WriteCommandType {
    set(Set::new),
    ttl(Ttl::new),
    setnx(SetNx::new),
    lpush(Lpush::new),
    lrem(Lrem::new),
    rpush(Rpush::new),
    del(Del::new),
    sadd(Sadd::new),//
    srem(Srem::new),
    hset(Hset::new),
    hdel(Hdel::new),//
    zadd(Zadd::new),
    zrem(Zrem::new),
    setex(SetEx::new),
    expire(Expire::new),
    incr(Incr::new),
    decr(Decr::new),
    mset(Mset::new),
    ;

    private final Supplier<WriteCommand> supplier;

    WriteCommandType(Supplier<WriteCommand> supplier) {
        this.supplier = supplier;
    }

    public static WriteCommandType getType(String commandName) {
        try {
            return valueOf(commandName.toLowerCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("不支持的命令类型: " + commandName, e);
        }
    }
}
