package com.wiqer.redis.command.impl.set;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.RedisSet;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class Scan extends AbstractCore<RedisSet> implements Command {

    @Override
    public String type() {
        return CommonCommandType.scan.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
    }

    @Override
    public void handle(ChannelHandlerContext ctx) {
        BulkString bulkString = BulkString.ZERO;
        List<BulkString> resp2 = keys().stream().map(BulkString::new).toList();
        ctx.writeAndFlush(List.of(bulkString, resp2));
    }
}
