package com.wiqer.redis.core;

import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisSet;

public class RedisSetCore extends RedisCore {

    public RedisSet get(BytesWrapper key) {
        return (RedisSet) super.get(key);
    }

}
