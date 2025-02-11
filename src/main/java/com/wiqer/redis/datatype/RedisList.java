package com.wiqer.redis.datatype;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * @author lilan
 */
public class RedisList extends RedisData {

    private final Deque<BytesWrapper> deque = new LinkedList<>();

    public void lpush(BytesWrapper... values) {
        for (BytesWrapper value : values) {
            deque.addFirst(value);
        }
    }

    public int size() {
        return deque.size();
    }

    public void lpush(List<BytesWrapper> values) {
        for (BytesWrapper value : values) {
            deque.addFirst(value);
        }
    }

    public void rpush(List<BytesWrapper> values) {
        for (BytesWrapper value : values) {
            deque.addLast(value);
        }
    }

    public List<BytesWrapper> lrang(int start, int end) {
        List<BytesWrapper> list = new ArrayList<>();
        long limit = end - start >= 0 ? end - start + 1 : 0;
        long toSkip = start;
        for (BytesWrapper bytesWrapper : deque) {
            if (toSkip > 0) {
                toSkip--;
                continue;
            }
            if (limit-- == 0) {
                break;
            }
            list.add(bytesWrapper);
        }
        return list;
    }

    public int remove(BytesWrapper value) {
        int count = 0;
        while (deque.remove(value)) {
            count++;
        }
        return count;
    }
}
