package com.wiqer.redis.command;

import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.resp.Resp;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author lilan
 */
public interface Command {

    Charset CHARSET = StandardCharsets.UTF_8;

    /**
     * 获取接口类型
     *
     * @return 接口类型
     */
    String type();

    /**
     * 注入属性
     *
     * @param array 操作数组
     */
    void init(RedisCore redisCore, List<Resp> array);

    /**
     * 处理消息命令
     *
     * @param ctx 管道
     */
    void handle(ChannelHandlerContext ctx);
}
