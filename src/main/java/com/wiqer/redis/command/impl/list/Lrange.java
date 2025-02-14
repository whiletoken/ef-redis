package com.wiqer.redis.command.impl.list;

import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisList;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class Lrange extends AbstractCore<RedisList> implements Command {

    private BytesWrapper key;
    private int start;
    private int end;

    @Override
    public String type() {
        return CommonCommandType.lrange.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
        key = ((BulkString) array.get(1)).getContent();
        start = Integer.parseInt(((BulkString) array.get(2)).getContent().toUtf8String());
        end = Integer.parseInt(((BulkString) array.get(3)).getContent().toUtf8String());
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        RedisList redisList = get(key);
        List<BytesWrapper> lrang = redisList.lrang(start, end);
        List<BulkString> respArray = lrang.stream().map(BulkString::new).toList();
        ctx.writeAndFlush(respArray);
    }
}
