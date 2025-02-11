package com.wiqer.redis.command.impl;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.core.RedisStringCore;
import com.wiqer.redis.datatype.RedisString;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class Config extends AbstractCore<RedisStringCore, RedisString> implements Command {

    private String param;

    @Override
    public String type() {
        return CommonCommandType.config.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        if (array.size() != 3) {
            throw new IllegalStateException();
        }
        if (!((BulkString) array.get(1)).getContent().toUtf8String().equals("get")) {
            throw new IllegalStateException();
        }
        param = ((BulkString) array.get(2)).getContent().toUtf8String();
        setRedisCore((RedisStringCore) redisCore);
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        if (param.equals("*") || param.equals("databases")) {
            List<BulkString> list = List.of(BulkString.DATABASES, BulkString.ONE);
            ctx.writeAndFlush(list);
        }
    }
}
