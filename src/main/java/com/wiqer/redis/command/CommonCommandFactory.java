package com.wiqer.redis.command;

import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.SimpleString;
import com.wiqer.redis.util.TraceIdUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class CommonCommandFactory {

    // 使用静态内部类实现单例模式，更加线程安全且延迟加载
    private static class SingletonHolder {
        private static volatile CommonCommandFactory instance;
    }

    private final RedisCore redisCore;

    public CommonCommandFactory(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    public static CommonCommandFactory create(RedisCore redisCore) {
        if (redisCore == null) {
            throw new IllegalArgumentException("RedisCore cannot be null");
        }

        if (SingletonHolder.instance == null) {
            synchronized (CommonCommandFactory.class) {
                if (SingletonHolder.instance == null) {
                    SingletonHolder.instance = new CommonCommandFactory(redisCore);
                }
            }
        }
        return SingletonHolder.instance;
    }

    public Command from(List<Resp> commands) {
        if (commands == null || commands.isEmpty()) {
            throw new IllegalArgumentException("Commands list cannot be null or empty");
        }

        Resp simpleString = commands.get(0);
        if (simpleString instanceof BulkString) {
            String commandName = ((BulkString) simpleString).getContent().toUtf8String();
            Command command = getCommand(commandName);
            if (command == null) {
                throw new UnsupportedOperationException("Unsupported command: " + commandName);
            }
            command.init(redisCore, commands);
            return command;
        }
        throw new IllegalArgumentException("Commands not supported");
    }

    private Command getCommand(String commandName) {
        try {
            return CommonCommandType.getType(commandName).getSupplier().get();
        } catch (Throwable e) {
            log.error("traceId:{} Unsupported command: {}, error: {}",
                    TraceIdUtil.currentTraceId(),
                    commandName,
                    e.getMessage()
            );
            return null;
        }
    }
}
