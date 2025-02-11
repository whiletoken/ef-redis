package com.wiqer.redis.datatype;

import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lilan
 */
@Getter
public class RedisHash extends RedisData {

    private final Map<BytesWrapper, BytesWrapper> map = new HashMap<>();

    public int put(BytesWrapper field, BytesWrapper value) {
        return map.put(field, value) == null ? 1 : 0;
    }

    public int del(List<BytesWrapper> fields) {
        return (int) fields.stream().filter(key -> map.remove(key) != null).count();
    }
}
