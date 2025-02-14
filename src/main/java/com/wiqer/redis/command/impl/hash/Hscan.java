package com.wiqer.redis.command.impl.hash;

import com.wiqer.redis.command.Command;
import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisHash;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Hscan extends AbstractCore<RedisHash> implements Command {

    private BytesWrapper key;

    @Override
    public String type() {
        return CommonCommandType.hscan.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
        key = ((BulkString) array.get(1)).getContent();
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        BulkString bulkString = BulkString.ZERO;
        RedisHash redisHash = get(key);
        Map<BytesWrapper, BytesWrapper> map = redisHash.getMap();
        List<Resp> list = map.entrySet().stream().flatMap(entry -> {
            Resp[] resps = new Resp[2];
            resps[0] = new BulkString(entry.getKey());
            resps[1] = new BulkString(entry.getValue());
            return Stream.of(resps);
        }).toList();
        ctx.writeAndFlush(List.of(bulkString, list));
    }

}
