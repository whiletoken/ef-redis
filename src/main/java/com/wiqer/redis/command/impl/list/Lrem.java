package com.wiqer.redis.command.impl.list;

import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.command.WriteCommandType;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.core.RedisListCore;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisList;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.RespInt;

import java.util.List;

public class Lrem extends AbstractCore<RedisListCore, RedisList> implements WriteCommand {

    private BytesWrapper key;
    private BytesWrapper value;

    @Override
    public String type() {
        return WriteCommandType.lrem.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore((RedisListCore) redisCore);
        key = ((BulkString) array.get(1)).getContent();
        value = ((BulkString) array.get(3)).getContent();
    }

    @Override
    public Resp handle() {
        RedisList redisList = get(key);
        int remove = redisList.remove(value);
        redisList.remove(value);
        return new RespInt(remove);
    }
}
