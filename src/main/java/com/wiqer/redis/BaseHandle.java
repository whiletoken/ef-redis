package com.wiqer.redis;

import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisData;

import java.util.List;
import java.util.Set;

public interface BaseHandle {

    Set<BytesWrapper> keys();

    boolean exist(BytesWrapper key);

    void put(BytesWrapper key, RedisData redisData);

    RedisData get(BytesWrapper key);

    long remove(List<BytesWrapper> keys);

}
