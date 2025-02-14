package com.wiqer.redis.command.impl;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisData;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import io.netty.channel.ChannelHandlerContext;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Info extends AbstractCore<RedisData> implements Command {

    @Override
    public String type() {
        return CommonCommandType.info.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        List<String> list = new ArrayList<>();
        list.add("redis_version:jfire_redis_mock");
        list.add("os:" + System.getProperty("os.name"));
        list.add("process_id:" + getPid());
        StringBuilder acc = new StringBuilder(list.get(0) + "\r\n");
        for (int i = 1; i < list.size(); i++) {
            String name = list.get(i);
            String string = name + "\r\n";
            acc.append(string);
        }
        Optional<String> reduce = Optional.of(acc.toString());
        String s = reduce.get();
        ctx.writeAndFlush(new BulkString(new BytesWrapper(s.getBytes(CHARSET))));
    }

    private String getPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        return name.split("@")[0];
    }
}
