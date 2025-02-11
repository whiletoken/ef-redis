package com.wiqer.redis.command.impl;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.WriteCommandType;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisData;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.RespInt;

import java.util.ArrayList;
import java.util.List;

public class Del extends AbstractCore<RedisCore, RedisData> implements WriteCommand {

    private List<BytesWrapper> keys;

    @Override
    public String type() {
        return WriteCommandType.del.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        List<BytesWrapper> list = new ArrayList<>();
        for (int i = 1, arrayLength = array.size(); i < arrayLength; i++) {
            Resp resp = array.get(i);
            BytesWrapper content = ((BulkString) resp).getContent();
            list.add(content);
        }
        keys = list;
        setRedisCore(redisCore);
    }

    @Override
    public Resp handle() {
        long remove = remove(keys);
        return new RespInt((int) remove);
    }
}
