package com.wiqer.redis.command.impl;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.RedisString;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.SimpleString;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class Auth extends AbstractCore<RedisString> implements Command {

    private String password;

    @Override
    public String type() {
        return CommonCommandType.auth.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        BulkString blukStrings = (BulkString) array.get(1);
        byte[] content = blukStrings.getContent().getByteArray();
        if (content.length == 0) {
            password = "";
        } else {
            password = new String(content);
        }
        setRedisCore(redisCore);
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(SimpleString.OK);
    }
}
