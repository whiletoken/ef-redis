package com.wiqer.redis.command.impl;

import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisString;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.SimpleString;
import com.wiqer.redis.util.Format;

import java.util.List;

public class Incr extends AbstractCore<RedisString> implements WriteCommand {

    private BytesWrapper key;

    @Override
    public String type() {
        return CommonCommandType.incr.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
        key = ((BulkString) array.get(1)).getContent();
    }

    @Override
    public Resp handle() {
        RedisString redisData = get(key);
        if (redisData == null) {
            put(key, RedisString.ZERO());
            return BulkString.ZERO;
        }
        try {
            BytesWrapper value = redisData.getValue();
            long v = Format.parseLong(value.getByteArray(), 10);
            ++v;
            BytesWrapper bytesWrapper = new BytesWrapper(Format.toByteArray(v));
            redisData.setValue(bytesWrapper);
            return new BulkString(bytesWrapper);
        } catch (Exception exception) {
            return new SimpleString("value is not an integer or out of range");
        }
    }
}
