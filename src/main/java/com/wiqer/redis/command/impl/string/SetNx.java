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
import com.wiqer.redis.resp.RespInt;

import java.util.List;

public class SetNx extends AbstractCore<RedisStringCore, RedisString> implements WriteCommand {

    private BytesWrapper key;

    private BytesWrapper value;

    @Override
    public String type() {
        return WriteCommandType.setnx.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore((RedisStringCore) redisCore);
        key = ((BulkString) array.get(1)).getContent();
        value = ((BulkString) array.get(2)).getContent();
    }

    @Override
    public Resp handle() {
        boolean exist = exist(key);
        if (!exist) {
            RedisString redisString = new RedisString();
            redisString.setValue(value);
            put(key, redisString);
            return RespInt.ONE;
        }
        return RespInt.ZERO;
    }
}
