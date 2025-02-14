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
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Slf4j
public class Keys extends AbstractCore<RedisData> implements Command {

    String pattern = "";

    @Override
    public String type() {
        return CommonCommandType.keys.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        //需要转译的字符(    [     {    /    ^    -    $     ¦    }    ]    )    ?    *    +    .
        pattern = "." + ((BulkString) array.get(1)).getContent().toUtf8String();
        setRedisCore(redisCore);
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        Set<BytesWrapper> keySet = keys();
        List<Resp> resps = keySet.stream().filter(k -> {
            String content = null;
            try {
                content = k.toUtf8String();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
            return Pattern.matches(pattern, content);
        }).flatMap(key -> {
            Resp[] info = new Resp[1];
            info[0] = new BulkString(key);
            return Stream.of(info);
        }).toList();
        ctx.writeAndFlush(resps);
    }
}