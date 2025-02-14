package com.wiqer.redis.datatype;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author lilan
 */
public class RedisZset extends RedisData {

    private final TreeMap<ZsetKey, Long> map =
            new TreeMap<>(Comparator.comparingLong((ZsetKey o) -> o.score)
                    .thenComparing(o -> o.key));

    public int add(List<ZsetKey> keys) {
        keys.forEach(key -> map.put(key, key.getScore()));
        return keys.size();
    }

    public List<ZsetKey> range(int start, int end) {
        if (start < 0 || end < start) {
            return List.of();
        }
        return map.keySet().stream()
                .skip(start)
                .limit(end - start + 1)
                .collect(Collectors.toList());
    }

    public List<ZsetKey> reRange(int start, int end) {
        if (start < 0 || end < start) {
            return List.of();
        }
        return map.descendingMap().keySet().stream()
                .skip(start)
                .limit(end - start + 1)
                .collect(Collectors.toList());
    }

    public int remove(List<BytesWrapper> members) {
        return (int) members.stream()
                .filter(member -> map.remove(new ZsetKey(member, 0)) != null)
                .count();
    }

    @Getter
    @EqualsAndHashCode
    public static class ZsetKey implements Comparable<ZsetKey> {

        BytesWrapper key;
        long score;

        public ZsetKey(BytesWrapper key, long score) {
            this.key = key;
            this.score = score;
        }

        @Override
        public int compareTo(ZsetKey other) {
            return this.key.compareTo(other.key);
        }
    }
}
