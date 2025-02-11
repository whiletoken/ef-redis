package com.wiqer.redis.command.impl;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisData;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.SimpleString;
import com.wiqer.redis.util.TraceIdUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class Client extends AbstractCore<RedisCore, RedisData> implements Command {

    private String subCommand;
    private List<Resp> array;

    @Override
    public String type() {
        return CommonCommandType.client.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
        this.array = array;
        subCommand = ((BulkString) array.get(1)).getContent().toUtf8String();
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        String traceId = TraceIdUtil.currentTraceId();
        log.debug("traceId:{} 当前的子命令是：{}", traceId, subCommand);
        if (!subCommand.equals("setname")) {
            throw new IllegalArgumentException();
        }
        BytesWrapper connectionName = ((BulkString) array.get(2)).getContent();
        putClient(connectionName, ctx.channel());
        TraceIdUtil.clear();
        ctx.writeAndFlush(SimpleString.OK);
    }
}
