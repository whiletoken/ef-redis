package com.wiqer.redis.core;

import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisZset;

public class RedisZsetCore extends RedisCore {

    public RedisZset get(BytesWrapper key) {
        return (RedisZset) super.get(key);
    }

}
