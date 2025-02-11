package com.wiqer.redis.command.impl.string;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.core.RedisStringCore;
import com.wiqer.redis.command.WriteCommandType;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisString;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.SimpleString;

import java.util.List;

public class SetEx extends AbstractCore<RedisStringCore, RedisString> implements WriteCommand {

    private BytesWrapper key;
    private int seconds;
    private BytesWrapper value;

    @Override
    public String type() {
        return WriteCommandType.setex.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        key = ((BulkString) array.get(1)).getContent();
        seconds = Integer.parseInt(((BulkString) array.get(2)).getContent().toUtf8String());
        value = ((BulkString) array.get(3)).getContent();
        setRedisCore((RedisStringCore) redisCore);
    }

    @Override
    public Resp handle() {
        RedisString redisString = new RedisString();
        redisString.setValue(value);
        redisString.setTimeout(System.currentTimeMillis() + (seconds * 1000L));
        put(key, redisString);
        return SimpleString.OK;
    }
}
