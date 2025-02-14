package com.wiqer.redis.command.impl;

import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisList;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Errors;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.RespInt;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public abstract class Push extends AbstractCore<RedisList> implements WriteCommand {

    BiConsumer<RedisList, List<BytesWrapper>> biConsumer;
    private BytesWrapper key;
    private List<BytesWrapper> value;

    public Push(BiConsumer<RedisList, List<BytesWrapper>> biConsumer) {
        this.biConsumer = biConsumer;
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
        key = ((BulkString) array.get(1)).getContent();
        value = new ArrayList<>();
        for (int i = 2; i < array.size(); i++) {
            value.add(((BulkString) array.get(i)).getContent());
        }
    }

    @Override
    public Resp handle() {
        RedisList redisData;
        try {
            redisData = get(key);
        } catch (Exception e) {
            return new Errors("wrong type");
        }
        if (redisData == null) {
            redisData = new RedisList();
        }
        biConsumer.accept(redisData, value);
        put(key, redisData);
        return new RespInt(redisData.size());
    }
}
