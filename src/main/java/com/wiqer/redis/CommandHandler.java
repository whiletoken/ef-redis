package com.wiqer.redis;

import com.wiqer.redis.command.Command;
import com.wiqer.redis.util.TraceIdUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommandHandler extends SimpleChannelInboundHandler<Command> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command command) {
        String traceId = TraceIdUtil.currentTraceId();
        log.debug("traceId: {},本次处理的命令：{}", traceId, command.type());
        try {
            command.handle(ctx);
        } catch (Exception e) {
            log.error("处理数据时", e);
        }
        TraceIdUtil.clear();
        log.debug("traceId:{}  命令处理完毕", traceId);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(" ExceptionCaught：", cause);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        ctx.close();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        ctx.flush();
    }
}
