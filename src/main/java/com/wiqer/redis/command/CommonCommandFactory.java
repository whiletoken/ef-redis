package com.wiqer.redis.command;

import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.SimpleString;
import com.wiqer.redis.util.TraceIdUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class CommonCommandFactory {

    public static final Object obj = new Object();
    private static volatile CommonCommandFactory factory;
    private final RedisCore redisCore;

    public CommonCommandFactory(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    public static CommonCommandFactory create(RedisCore redisCore) {
        if (factory == null) {
            synchronized (obj) {
                if (factory == null) {
                    factory = new CommonCommandFactory(redisCore);
                }
            }
        }
        return factory;
    }

    public Command from(List<Resp> string) {
        SimpleString simpleString = (SimpleString) string.get(0);
        String commandName = simpleString.getContent().toLowerCase();
        Command command = getCommand(commandName);
        command.init(factory.redisCore, string);
        return command;
    }

    private Command getCommand(String commandName) {
        try {
            return CommonCommandType.getType(commandName).getSupplier().get();
        } catch (Throwable e) {
            log.debug("traceId:{} 不支持的命令：{},数据读取异常", TraceIdUtil.currentTraceId(), commandName);
            return null;
        }
    }
}
