package com.wiqer.redis.command.impl;

import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisData;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.RespInt;

import java.util.List;

public class Expire extends AbstractCore<RedisData> implements WriteCommand {

    private BytesWrapper key;

    private int second;

    @Override
    public String type() {
        return CommonCommandType.expire.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
        key = ((BulkString) array.get(1)).getContent();
        second = Integer.parseInt(((BulkString) array.get(2)).getContent().toUtf8String());
    }

    @Override
    public Resp handle() {
        RedisData redisData = get(key);
        if (redisData == null) {
            return RespInt.ZERO;
        }
        redisData.setTimeout(System.currentTimeMillis() + (second * 1000L));
        return RespInt.ONE;
    }
}
