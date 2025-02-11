package com.wiqer.redis.command.impl.string;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.core.RedisStringCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisString;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import io.netty.channel.ChannelHandlerContext;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Mget extends AbstractCore<RedisStringCore, RedisString> implements Command {

    private List<BytesWrapper> keys;

    @Override
    public String type() {
        return CommonCommandType.mget.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        keys = Stream.of(array).skip(1).map(resp -> ((BulkString) resp).getContent()).collect(Collectors.toList());
        setRedisCore((RedisStringCore) redisCore);
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        LinkedList<BytesWrapper> linkedList = new LinkedList<>();
        keys.forEach(key -> {
            RedisString redisData = get(key);
            if (redisData != null) {
                linkedList.add(redisData.getValue());
            }
        });
        List<BulkString> respArray = linkedList.stream().map(BulkString::new).toList();
        ctx.writeAndFlush(respArray);
    }

}
