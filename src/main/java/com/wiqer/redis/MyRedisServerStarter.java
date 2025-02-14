package com.wiqer.redis;

import com.wiqer.redis.netty.channel.single.NettySingleSelectChannelOption;
import lombok.extern.slf4j.Slf4j;

/**
 * Redis服务器启动类
 *
 * @author Administrator
 */
@Slf4j
public class MyRedisServerStarter {

    public static void main(String[] args) {

        try (MyRedisServer server = new MyRedisServer(new NettySingleSelectChannelOption())) {

            server.start();
            log.info("Redis服务器启动成功");

            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("正在关闭Redis服务器...");
                server.close();
                log.info("Redis服务器已关闭");
            }));

            // 等待服务器运行
            Thread.currentThread().join();

        } catch (Exception e) {
            log.error("Redis服务器运行异常", e);
            System.exit(1);
        }
    }
}
