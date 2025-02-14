package com.wiqer.redis.command.impl.string;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisString;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class Get extends AbstractCore<RedisString> implements Command {

    private BytesWrapper key;

    @Override
    public String type() {
        return CommonCommandType.get.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        this.key = ((BulkString) array.get(1)).getContent();
        setRedisCore(redisCore);
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        RedisString redisData = get(key);
        if (redisData == null) {
            ctx.writeAndFlush(BulkString.NullBulkString);
        } else {
            BytesWrapper value = redisData.getValue();
            ctx.writeAndFlush(new BulkString(value));
        }
    }
}
