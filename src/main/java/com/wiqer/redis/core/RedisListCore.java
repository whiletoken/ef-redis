package com.wiqer.redis.core;

import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisList;

public class RedisListCore extends RedisCore {

    public RedisList get(BytesWrapper key) {
        return (RedisList) super.get(key);
    }

}
