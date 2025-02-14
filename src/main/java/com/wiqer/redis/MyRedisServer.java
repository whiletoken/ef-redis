package com.wiqer.redis;

import com.wiqer.redis.aof.Aof;
import com.wiqer.redis.channel.DefaultChannelSelectStrategy;
import com.wiqer.redis.channel.LocalChannelOption;
import com.wiqer.redis.channel.single.NettySingleSelectChannelOption;
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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

/**
 * @author Administrator
 */
@Slf4j
public class MyRedisServer implements RedisServer, AutoCloseable {
    private static final int DEFAULT_BACKLOG = 1024;
    private static final int DEFAULT_THREAD_COUNT = 1;
    private static final String SERVER_START_SUCCESS = "Redis服务器启动成功 - 监听地址: {}";
    private static final String SERVER_START_FAILED = "Redis服务器启动失败";
    private static final String SERVER_SHUTDOWN = "Redis服务器正在关闭...";

    @Getter
    private final RedisCore redisCore;
    private final ServerBootstrap serverBootstrap;
    private final EventExecutorGroup redisSingleEventExecutor;
    private final LocalChannelOption channelOption;
    private volatile boolean isRunning;
    private ChannelFuture serverChannelFuture;
    private Aof aof;

    public MyRedisServer() {
        this(new DefaultChannelSelectStrategy().select());
    }

    public MyRedisServer(LocalChannelOption channelOption) {
        this.redisCore = new RedisCore();
        this.serverBootstrap = new ServerBootstrap();
        this.channelOption = channelOption;
        this.redisSingleEventExecutor = createEventExecutor();
        this.isRunning = false;
    }

    private EventExecutorGroup createEventExecutor() {
        return new NioEventLoopGroup(DEFAULT_THREAD_COUNT, new ThreadFactory() {
            private final AtomicInteger threadCount = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "Redis-Worker-" + threadCount.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    @Override
    public void start() {
        if (isRunning) {
            log.warn("服务器已经在运行中");
            return;
        }
        initializeAof();
        configureAndStartServer();
    }

    private void initializeAof() {
        if (PropertiesUtil.getAppendOnly()) {
            aof = new Aof(this.redisCore);
        }
    }

    private void configureAndStartServer() {
        try {
            serverBootstrap.group(channelOption.boss(), channelOption.selectors())
                    .channel(channelOption.getChannelClass())
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .option(ChannelOption.SO_BACKLOG, DEFAULT_BACKLOG)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.SO_KEEPALIVE, PropertiesUtil.getTcpKeepAlive())
                    .localAddress(new InetSocketAddress(
                            PropertiesUtil.getNodeAddress(),
                            PropertiesUtil.getNodePort()))
                    .childHandler(createChannelInitializer());

            serverChannelFuture = serverBootstrap.bind().sync();
            isRunning = true;
            log.info(SERVER_START_SUCCESS, serverChannelFuture.channel().localAddress());
            serverChannelFuture.channel().closeFuture().sync().channel();
        } catch (Exception e) {
            log.error(SERVER_START_FAILED, e);
            close();
            throw new RuntimeException(SERVER_START_FAILED, e);
        }
    }

    private ChannelInitializer<SocketChannel> createChannelInitializer() {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(SocketChannel socketChannel) {
                ChannelPipeline pipeline = socketChannel.pipeline();
                pipeline.addLast(
                        new ResponseEncoder(),
                        new CommandDecoder(redisCore, aof)
                );
                pipeline.addLast(redisSingleEventExecutor, new CommandHandler());
            }
        };
    }

    @Override
    public void close() {
        if (!isRunning) {
            return;
        }
        log.info(SERVER_SHUTDOWN);
        isRunning = false;
        try {
            if (serverChannelFuture != null) {
                serverChannelFuture.channel().close().sync();
            }
            shutdownExecutors();
            if (aof != null) {
                aof.close();
            }
        } catch (Exception e) {
            log.error("关闭服务器时发生错误", e);
        }
    }

    private void shutdownExecutors() {
        gracefulShutdown(channelOption.boss(), "Boss EventLoopGroup");
        gracefulShutdown(channelOption.selectors(), "Worker EventLoopGroup");
        gracefulShutdown(redisSingleEventExecutor, "Single EventExecutor");
    }

    private void gracefulShutdown(EventExecutorGroup executor, String executorName) {
        if (executor != null && !executor.isShutdown()) {
            try {
                executor.shutdownGracefully()
                        .sync()
                        .await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.warn("{} 关闭被中断", executorName, e);
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args) {
        try (MyRedisServer server = new MyRedisServer(new NettySingleSelectChannelOption())) {
            server.start();
            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        }
    }

    @Override
    public RedisCore getRedisCore() {
        return redisCore;
    }
}
