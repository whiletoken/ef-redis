package com.wiqer.redis.command.impl;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.RedisData;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.SimpleString;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class Select extends AbstractCore<RedisData> implements Command {

    private Integer index;

    @Override
    public String type() {
        return CommonCommandType.select.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
        index = Integer.parseInt(((BulkString) array.get(1)).getContent().toUtf8String());
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(SimpleString.OK);
    }

}