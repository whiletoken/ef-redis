package com.wiqer.redis.command.impl.set;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.core.RedisSetCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisSet;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class Sscan extends AbstractCore<RedisSetCore, RedisSet> implements Command {

    private BytesWrapper key;

    @Override
    public String type() {
        return CommonCommandType.sscan.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore((RedisSetCore) redisCore);
        key = ((BulkString) array.get(1)).getContent();
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        RedisSet redisSet = get(key);
        List<BulkString> list = redisSet.keys().stream().map(BulkString::new).toList();
        BulkString bulkString = BulkString.ZERO;
        ctx.writeAndFlush(List.of(bulkString, list));
    }

}
