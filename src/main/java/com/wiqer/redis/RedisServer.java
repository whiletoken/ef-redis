package com.wiqer.redis;

import com.wiqer.redis.core.RedisCore;

public interface RedisServer {

    void start();

    void close();

    RedisCore getRedisCore();
}
