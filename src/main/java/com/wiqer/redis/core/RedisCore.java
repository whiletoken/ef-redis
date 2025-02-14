package com.wiqer.redis.core;

import com.wiqer.redis.BaseHandle;
import com.wiqer.redis.datatype.*;
import io.netty.channel.Channel;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author lilan
 */
public class RedisCore implements BaseHandle {

    /**
     * 客户端可能使用hash路由，更换为跳表更好的避免hash冲突
     */
    private final ConcurrentNavigableMap<BytesWrapper, RedisData> map =
            new ConcurrentSkipListMap<>();

    private final ConcurrentHashMap<BytesWrapper, Channel> clients =
            new ConcurrentHashMap<>();

    private final Map<Channel, BytesWrapper> clientNames = new ConcurrentHashMap<>();

    @Override
    public Set<BytesWrapper> keys() {
        return map.keySet();
    }

    public void putClient(BytesWrapper connectionName, Channel channelContext) {
        clients.put(connectionName, channelContext);
        clientNames.put(channelContext, connectionName);
    }

    @Override
    public boolean exist(BytesWrapper key) {
        return map.containsKey(key);
    }

    @Override
    public void put(BytesWrapper key, RedisData redisData) {
        map.put(key, redisData);
    }

    /**
     * 优化过期键的清理逻辑
     */
    @Override
    public RedisData get(BytesWrapper key) {
        RedisData redisData = map.get(key);
        if (redisData == null || isExpired(redisData)) {
            if (redisData != null) {
                map.remove(key);
            }
            return null;
        }
        return redisData;
    }

    /**
     * 判断数据是否过期
     */
    private boolean isExpired(RedisData redisData) {
        long timeout = redisData.timeout();
        return timeout != -1 && timeout < System.currentTimeMillis();
    }

    @Override
    public long remove(List<BytesWrapper> keys) {
        return keys.stream()
                .filter(key -> map.remove(key) != null)
                .count();
    }

    public void cleanAll() {
        map.clear();
    }
}
