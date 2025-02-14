package com.wiqer.redis.command;

import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.util.TraceIdUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class WriteCommandFactory {

    private static volatile WriteCommandFactory INSTANCE;
    private final RedisCore redisCore;

    private WriteCommandFactory(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    public static WriteCommandFactory create(RedisCore redisCore) {
        if (INSTANCE == null) {
            synchronized (WriteCommandFactory.class) {
                if (INSTANCE == null) {
                    INSTANCE = new WriteCommandFactory(redisCore);
                }
            }
        }
        return INSTANCE;
    }

    public WriteCommand from(List<Resp> arrays) {
        if (arrays == null || arrays.isEmpty()) {
            throw new IllegalArgumentException("命令数组不能为空");
        }

        Resp firstElement = arrays.get(0);
        if (!(firstElement instanceof BulkString)) {
            throw new IllegalArgumentException("命令格式错误");
        }

        String commandName = ((BulkString) firstElement).getContent().toUtf8String().toLowerCase();
        WriteCommand command = createCommand(commandName);
        if (command == null) {
            throw new UnsupportedOperationException("不支持的命令：" + commandName);
        }

        command.init(redisCore, arrays);
        return command;
    }

    private WriteCommand createCommand(String commandName) {
        try {
            return WriteCommandType.getType(commandName).getSupplier().get();
        } catch (Exception e) {
            log.error("traceId:{} 创建命令失败：{}", TraceIdUtil.currentTraceId(), commandName, e);
            return null;
        }
    }
}
