package com.wiqer.redis.core;

import com.wiqer.redis.BaseHandle;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisData;
import io.netty.channel.Channel;
import lombok.Setter;

import java.util.List;
import java.util.Set;

@Setter
public abstract class AbstractCore<T extends RedisCore, E extends RedisData> implements BaseHandle {

    private T redisCore;

    /**
     * 客户端连接管理
     */
    public void putClient(BytesWrapper connectionName, Channel channelContext) {
        if (connectionName == null || channelContext == null) {
            throw new IllegalArgumentException("连接名称和通道上下文不能为空");
        }
        redisCore.putClient(connectionName, channelContext);
    }

    @Override
    public E get(BytesWrapper key) {
        return (E) redisCore.get(key);
    }

    @Override
    public void put(BytesWrapper key, RedisData redisData) {
        redisCore.put(key, redisData);
    }

    @Override
    public boolean exist(BytesWrapper key) {
        return redisCore.exist(key);
    }

    @Override
    public Set<BytesWrapper> keys() {
        return redisCore.keys();
    }

    /**
     * 批量删除优化
     */
    @Override
    public long remove(List<BytesWrapper> keys) {
        if (keys == null || keys.isEmpty()) {
            return 0;
        }
        return redisCore.remove(keys);
    }

}
