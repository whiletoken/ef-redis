package com.wiqer.redis.datatype;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author lilan
 */
public class RedisZset extends RedisData {

    private final TreeMap<ZsetKey, Long> map = new TreeMap<>((o1, o2) -> {
        if (o1.key.equals(o2.key)) {
            return 0;
        }
        return Long.compare(o1.score, o2.score);
    });

    public int add(List<ZsetKey> keys) {
        for (ZsetKey key : keys) {
            map.put(key, key.getScore());
        }
        return keys.size();
    }

    public List<ZsetKey> range(int start, int end) {
        return map.keySet().stream().skip(start).limit(end - start >= 0 ? end - start + 1 : 0).collect(Collectors.toList());
    }

    public List<ZsetKey> reRange(int start, int end) {
        return map.descendingKeySet().descendingSet().stream().skip(start).limit(end - start >= 0 ? end - start + 1 : 0).collect(Collectors.toList());
    }

    public int remove(List<BytesWrapper> members) {
        return (int) members.stream().filter(member -> map.remove(new ZsetKey(member, 0)) != null).count();
    }

    @Getter
    @EqualsAndHashCode
    public static class ZsetKey {

        BytesWrapper key;

        long score;

        public ZsetKey(BytesWrapper key, long score) {
            this.key = key;
            this.score = score;
        }
    }
}
