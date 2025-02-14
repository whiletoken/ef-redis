package com.wiqer.redis.command.impl.zset;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisZset;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;
import java.util.stream.Stream;

public class Zrevrange extends AbstractCore<RedisZset> implements Command {

    private BytesWrapper key;
    private int start;
    private int end;

    @Override
    public String type() {
        return CommonCommandType.zrevrange.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        this.key = ((BulkString) array.get(1)).getContent();
        this.start = Integer.parseInt(((BulkString) array.get(2)).getContent().toUtf8String());
        this.end = Integer.parseInt(((BulkString) array.get(3)).getContent().toUtf8String());
        setRedisCore(redisCore);
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        RedisZset redisZset = get(key);
        List<RedisZset.ZsetKey> keys = redisZset.reRange(start, end);
        List<Resp> resps = keys.stream().flatMap(key -> {
            Resp[] info = new Resp[2];
            info[0] = new BulkString(key.getKey());
            info[1] = new BulkString(new BytesWrapper(String.valueOf(key.getScore()).getBytes(CHARSET)));
            return Stream.of(info);
        }).toList();
        ctx.writeAndFlush(resps);
    }
}
