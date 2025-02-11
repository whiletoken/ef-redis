package com.wiqer.redis.command.impl.zset;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.core.RedisZsetCore;
import com.wiqer.redis.command.WriteCommandType;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisZset;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.RespInt;

import java.util.ArrayList;
import java.util.List;

public class Zadd extends AbstractCore<RedisZsetCore, RedisZset> implements WriteCommand {

    private BytesWrapper key;

    private List<RedisZset.ZsetKey> keys;

    @Override
    public String type() {
        return WriteCommandType.zadd.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore((RedisZsetCore) redisCore);
        this.key = ((BulkString) array.get(1)).getContent();
        this.keys = new ArrayList<>();
        for (int i = 2; i + 1 < array.size(); i += 2) {
            long score = Long.parseLong(((BulkString) array.get(i)).getContent().toUtf8String());
            BytesWrapper member = ((BulkString) array.get(i + 1)).getContent();
            this.keys.add(new RedisZset.ZsetKey(member, score));
        }
    }

    @Override
    public Resp handle() {
        RedisZset redisData = get(key);
        int add;
        if (redisData == null) {
            RedisZset redisZset = new RedisZset();
            add = redisZset.add(keys);
            put(key, redisZset);
        } else {
            add = redisData.add(keys);
        }
        return new RespInt(add);
    }
}
