package com.wiqer.redis.datatype;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @author lilan
 */
public class RedisSet extends RedisData {

    private final Set<BytesWrapper> set = new HashSet<>();

    public int sadd(List<BytesWrapper> members) {
        if (members == null || members.isEmpty()) {
            return 0;
        }
        return (int) members.stream()
                .filter(Objects::nonNull)
                .filter(set::add)
                .count();
    }

    public int srem(List<BytesWrapper> members) {
        if (members == null || members.isEmpty()) {
            return 0;
        }
        return (int) members.stream()
                .filter(Objects::nonNull)
                .filter(set::remove)
                .count();
    }

    public boolean sismember(BytesWrapper member) {
        return member != null && set.contains(member);
    }

    public Collection<BytesWrapper> smembers() {
        return Collections.unmodifiableCollection(set);
    }

    public int scard() {
        return set.size();
    }

    public Collection<BytesWrapper> keys() {
        return smembers();
    }
}
