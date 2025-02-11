package com.wiqer.redis.command;

import com.wiqer.redis.resp.Resp;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author lilan
 */
public interface WriteCommand extends Command {

    /**
     * for aof
     */
    Resp handle();

    @Override
    default void handle(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(handle());
    }

}
