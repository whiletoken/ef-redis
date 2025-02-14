package com.wiqer.redis.command;

import com.wiqer.redis.command.impl.*;
import com.wiqer.redis.command.impl.hash.Hdel;
import com.wiqer.redis.command.impl.hash.Hscan;
import com.wiqer.redis.command.impl.hash.Hset;
import com.wiqer.redis.command.impl.list.Lpush;
import com.wiqer.redis.command.impl.list.Lrange;
import com.wiqer.redis.command.impl.list.Lrem;
import com.wiqer.redis.command.impl.set.Sadd;
import com.wiqer.redis.command.impl.set.Scan;
import com.wiqer.redis.command.impl.set.Srem;
import com.wiqer.redis.command.impl.set.Sscan;
import com.wiqer.redis.command.impl.string.*;
import com.wiqer.redis.command.impl.zset.Zadd;
import com.wiqer.redis.command.impl.zset.Zrem;
import com.wiqer.redis.command.impl.zset.Zrevrange;
import lombok.Getter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Getter
public enum CommonCommandType {
    auth(Auth::new),
    config(Config::new),
    scan(Scan::new),//
    info(Info::new),
    client(Client::new),
    type(Type::new),//
    get(Get::new),
    quit(Quit::new),//
    lrange(Lrange::new),
    sscan(Sscan::new),
    hscan(Hscan::new),
    zrevrange(Zrevrange::new),
    exists(Exists::new),
    ping(Ping::new),
    select(Select::new),
    keys(Keys::new),
    mget(Mget::new),

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

    private final Supplier<Command> supplier;

    CommonCommandType(Supplier<Command> supplier) {
        this.supplier = supplier;
    }

    private static final Map<String, CommonCommandType> COMMAND_MAP;

    static {
        COMMAND_MAP = Arrays.stream(values())
                .collect(Collectors.toMap(
                        CommonCommandType::name,
                        Function.identity(),
                        (existing, replacement) -> existing,
                        HashMap::new
                ));
    }

    public static CommonCommandType getType(String commandName) {
        if (commandName == null) {
            throw new IllegalArgumentException("Command name cannot be null");
        }
        CommonCommandType type = COMMAND_MAP.get(commandName.toLowerCase());
        if (type == null) {
            throw new IllegalArgumentException("Unsupported command: " + commandName);
        }
        return type;
    }
}
