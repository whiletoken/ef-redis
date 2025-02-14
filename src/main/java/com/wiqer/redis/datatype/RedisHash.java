package com.wiqer.redis.datatype;

import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author lilan
 */
@Getter
public class RedisHash extends RedisData {

    private final Map<BytesWrapper, BytesWrapper> map = new HashMap<>();

    public int put(BytesWrapper field, BytesWrapper value) {
        if (field == null || value == null) {
            return 0;
        }
        return map.put(field, value) == null ? 1 : 0;
    }

    public BytesWrapper get(BytesWrapper field) {
        return map.get(field);
    }

    public int del(List<BytesWrapper> fields) {
        if (fields == null || fields.isEmpty()) {
            return 0;
        }
        return (int) fields.stream()
                .filter(Objects::nonNull)
                .filter(key -> map.remove(key) != null)
                .count();
    }

    public long len() {
        return map.size();
    }

    public boolean exists(BytesWrapper field) {
        return map.containsKey(field);
    }
}
