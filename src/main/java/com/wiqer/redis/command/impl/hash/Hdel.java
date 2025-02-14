package com.wiqer.redis.command.impl.hash;

import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisHash;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.RespInt;

import java.util.List;
import java.util.stream.Collectors;

public class Hdel extends AbstractCore<RedisHash> implements WriteCommand {

    private BytesWrapper key;
    private List<BytesWrapper> fields;

    @Override
    public String type() {
        return CommonCommandType.hdel.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        if (array == null || array.size() < 3) {
            throw new IllegalArgumentException("HDEL命令至少需要一个key和一个field");
        }

        setRedisCore(redisCore);
        key = ((BulkString) array.get(1)).getContent();

        // 使用更高效的方式收集fields
        fields = array.subList(2, array.size()).stream()
                .map(resp -> {
                    if (!(resp instanceof BulkString)) {
                        throw new IllegalArgumentException("HDEL命令参数必须是字符串");
                    }
                    return ((BulkString) resp).getContent();
                })
                .collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        if (key == null || fields.isEmpty()) {
            return RespInt.ZERO;
        }

        RedisHash redisHash = get(key);
        if (redisHash == null) {
            return RespInt.ZERO;
        }

        return new RespInt(redisHash.del(fields));
    }
}
