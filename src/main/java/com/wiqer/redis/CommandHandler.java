package com.wiqer.redis;

import com.wiqer.redis.command.Command;
import com.wiqer.redis.resp.Errors;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class CommandHandler extends SimpleChannelInboundHandler<Command> {
    private static final String COMMAND_EXECUTION_ERROR = "命令执行出错: {}";
    private static final String CHANNEL_INACTIVE_MESSAGE = "Channel 已断开连接";
    private static final String EXCEPTION_CAUGHT_MESSAGE = "异常被捕获";

    // 添加连接统计
    private static final AtomicInteger ACTIVE_CONNECTIONS = new AtomicInteger(0);
    private static final int MAX_CONNECTIONS = 10000;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command command) {
        if (command == null) {
            log.warn("收到空命令");
            return;
        }
        try {
            processCommand(ctx, command);
        } catch (Exception e) {
            handleCommandError(ctx, e);
        }
    }

    private void processCommand(ChannelHandlerContext ctx, Command command) {
        command.handle(ctx);
    }

    private void handleCommandError(ChannelHandlerContext ctx, Exception e) {
        log.error(COMMAND_EXECUTION_ERROR, e.getMessage());
        ctx.writeAndFlush(new Errors(e.getMessage()));
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (ACTIVE_CONNECTIONS.incrementAndGet() > MAX_CONNECTIONS) {
            log.warn("达到最大连接数限制: {}", MAX_CONNECTIONS);
            ctx.close();
            return;
        }
        log.info("新连接建立 - 远程地址: {}", ctx.channel().remoteAddress());
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ACTIVE_CONNECTIONS.decrementAndGet();
        log.info(CHANNEL_INACTIVE_MESSAGE + " - 远程地址: {}", ctx.channel().remoteAddress());
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(EXCEPTION_CAUGHT_MESSAGE, cause);
        ctx.close();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (!ctx.channel().isWritable()) {
            ctx.channel().flush();
        }
        ctx.fireChannelWritabilityChanged();
    }

    // 添加监控方法
    public static int getActiveConnections() {
        return ACTIVE_CONNECTIONS.get();
    }
}
