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

public class Ttl extends AbstractCore<RedisData> implements WriteCommand {

    private BytesWrapper key;

    @Override
    public String type() {
        return CommonCommandType.ttl.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
        key = ((BulkString) array.get(1)).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = get(key);
        if (redisData == null) {
            return new RespInt(-2);
        } else if (redisData.timeout() == -1) {
            return new RespInt(-1);
        } else {
            long second = (redisData.timeout() - System.currentTimeMillis()) / 1000;
            return new RespInt((int) second);
        }
    }
}
