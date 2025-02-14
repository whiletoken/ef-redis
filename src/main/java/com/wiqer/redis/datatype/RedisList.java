package com.wiqer.redis.datatype;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author lilan
 */
public class RedisList extends RedisData {

    private final Deque<BytesWrapper> deque = new LinkedList<>();

    public void lpush(BytesWrapper... values) {
        if (values == null) {
            return;
        }
        for (BytesWrapper value : values) {
            if (value != null) {
                deque.addFirst(value);
            }
        }
    }

    public int size() {
        return deque.size();
    }

    public void lpush(List<BytesWrapper> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        values.stream()
                .filter(Objects::nonNull)
                .forEach(deque::addFirst);
    }

    public void rpush(List<BytesWrapper> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        values.stream()
                .filter(Objects::nonNull)
                .forEach(deque::addLast);
    }

    public List<BytesWrapper> lrang(int start, int end) {
        if (start < 0 || end < start || deque.isEmpty()) {
            return Collections.emptyList();
        }
        return deque.stream()
                .skip(start)
                .limit(end - start + 1L)
                .collect(Collectors.toList());
    }

    public int remove(BytesWrapper value) {
        if (value == null) {
            return 0;
        }
        int count = 0;
        while (deque.remove(value)) {
            count++;
        }
        return count;
    }
}
