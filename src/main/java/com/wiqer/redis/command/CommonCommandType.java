package com.wiqer.redis.command;

import com.wiqer.redis.command.impl.*;
import com.wiqer.redis.command.impl.hash.Hscan;
import com.wiqer.redis.command.impl.list.Lrange;
import com.wiqer.redis.command.impl.set.Scan;
import com.wiqer.redis.command.impl.set.Sscan;
import com.wiqer.redis.command.impl.string.*;
import com.wiqer.redis.command.impl.zset.Zrevrange;
import lombok.Getter;

import java.util.function.Supplier;

@Getter
public enum CommonCommandType  {
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
    ;

    private final Supplier<Command> supplier;

    CommonCommandType(Supplier<Command> supplier) {
        this.supplier = supplier;
    }

    public static CommonCommandType getType(String commandName) {
        for (CommonCommandType value : values()) {
            if (value.name().equals(commandName)) {
                return value;
            }
        }
        throw new RuntimeException("command not found");
    }
}
