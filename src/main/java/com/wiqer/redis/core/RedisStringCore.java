package com.wiqer.redis.core;

import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisString;

public class RedisStringCore extends RedisCore {

    public RedisString get(BytesWrapper key) {
        return (RedisString) super.get(key);
    }

}
