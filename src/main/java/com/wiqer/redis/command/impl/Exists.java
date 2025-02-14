package com.wiqer.redis.command.impl;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisData;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.RespInt;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class Exists extends AbstractCore<RedisData> implements Command {

    private BytesWrapper key;

    @Override
    public String type() {
        return CommonCommandType.exists.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
        key = ((BulkString) array.get(1)).getContent();
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        boolean exist = exist(key);
        if (exist) {
            ctx.writeAndFlush(new RespInt(1));
        } else {
            ctx.writeAndFlush(new RespInt(0));
        }
    }
}
