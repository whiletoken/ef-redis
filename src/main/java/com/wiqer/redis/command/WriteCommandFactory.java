package com.wiqer.redis.command;

import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.util.TraceIdUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class WriteCommandFactory {

    public static final Object obj = new Object();
    private static volatile WriteCommandFactory factory;
    private final RedisCore redisCore;

    public WriteCommandFactory(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    public static WriteCommandFactory create(RedisCore redisCore) {
        if (factory == null) {
            synchronized (obj) {
                if (factory == null) {
                    factory = new WriteCommandFactory(redisCore);
                }
            }
        }
        return factory;
    }

    public WriteCommand from(List<Resp> arrays) {
        String commandName = ((BulkString) arrays.get(0)).getContent().toUtf8String().toLowerCase();
        WriteCommand command = getCommand(commandName);
        command.init(factory.redisCore, arrays);
        return command;
    }

    private WriteCommand getCommand(String commandName) {
        try {
            return WriteCommandType.getType(commandName).getSupplier().get();
        } catch (Throwable e) {
            log.debug("traceId:{} 不支持的命令：{},数据读取异常", TraceIdUtil.currentTraceId(), commandName);
            return null;
        }
    }
}
