package com.wiqer.redis;

import com.wiqer.redis.aof.Aof;
import com.wiqer.redis.channel.DefaultChannelSelectStrategy;
import com.wiqer.redis.channel.LocalChannelOption;
import com.wiqer.redis.channel.single.NettySingleSelectChannelOption;
import com.wiqer.redis.command.CommonCommandFactory;
import com.wiqer.redis.command.WriteCommandFactory;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.util.PropertiesUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;

/**
 * @author Administrator
 */
@Slf4j
public class MyRedisServer implements RedisServer {

    private final RedisCore redisCore = new RedisCore();
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    private final EventExecutorGroup redisSingleEventExecutor;
    private final LocalChannelOption channelOption;
    private Aof aof;
    private CommonCommandFactory commonCommandFactory;
    private WriteCommandFactory writeCommandFactory;

    public MyRedisServer() {
        channelOption = new DefaultChannelSelectStrategy().select();
        this.redisSingleEventExecutor = new NioEventLoopGroup(1);
    }

    public MyRedisServer(LocalChannelOption channelOption) {
        this.channelOption = channelOption;
        this.redisSingleEventExecutor = new NioEventLoopGroup(1);
    }

    public static void main(String[] args) {
        new MyRedisServer(new NettySingleSelectChannelOption()).start();
    }

    @Override
    public void start() {
        if (PropertiesUtil.getAppendOnly()) {
            aof = new Aof(this.redisCore);
        }
        start0();
    }

    @Override
    public void close() {
        try {
            channelOption.boss().shutdownGracefully();
            channelOption.selectors().shutdownGracefully();
            redisSingleEventExecutor.shutdownGracefully();
        } catch (Exception exception) {
            log.error("Exception!", exception);
        }
    }

    public void start0() {
        serverBootstrap.group(channelOption.boss(), channelOption.selectors())
                .channel(channelOption.getChannelClass())
                .handler(new LoggingHandler(LogLevel.INFO))
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, PropertiesUtil.getTcpKeepAlive())
                .localAddress(new InetSocketAddress(PropertiesUtil.getNodeAddress(), PropertiesUtil.getNodePort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) {
                        ChannelPipeline channelPipeline = socketChannel.pipeline();
                        channelPipeline.addLast(
                                new ResponseEncoder(),
                                new CommandDecoder(aof)
                        );
                        channelPipeline.addLast(redisSingleEventExecutor, new CommandHandler());
                    }
                });
        try {
            ChannelFuture sync = serverBootstrap.bind().sync();
            log.info(sync.channel().localAddress().toString());
        } catch (InterruptedException e) {
            log.warn("Interrupted!", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public RedisCore getRedisCore() {
        return redisCore;
    }
}
