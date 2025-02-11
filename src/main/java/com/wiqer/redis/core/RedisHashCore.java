package com.wiqer.redis.core;

import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisHash;

public class RedisHashCore extends RedisCore {

    public RedisHash get(BytesWrapper key) {
        return (RedisHash) super.get(key);
    }

}
