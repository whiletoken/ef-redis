package com.wiqer.redis.command.impl;

import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.datatype.RedisData;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.SimpleString;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class Ping extends AbstractCore<RedisData> implements Command {

    @Override
    public String type() {
        return CommonCommandType.lrem.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(SimpleString.PONG);
    }
}
