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

public class Hset extends AbstractCore<RedisHash> implements WriteCommand {

    private BytesWrapper key;
    private BytesWrapper field;
    private BytesWrapper value;

    @Override
    public String type() {
        return CommonCommandType.hset.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
        key = ((BulkString) array.get(1)).getContent();
        field = ((BulkString) array.get(2)).getContent();
        value = ((BulkString) array.get(3)).getContent();
    }

    @Override
    public Resp handle() {
        RedisHash redisData = get(key);
        int put;
        if (redisData == null) {
            RedisHash redisHash = new RedisHash();
            put = redisHash.put(field, value);
            redisHash.put(field, value);
            put(key, redisHash);
        } else {
            put = redisData.put(field, value);
            redisData.put(field, value);
        }
        return new RespInt(put);
    }
}
