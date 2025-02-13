package com.wiqer.redis.aof.queue;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class RingBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E>, Serializable {
    static final int MAXIMUM_CAPACITY = 1 << 29;
    static final int MAXIMUM_SUBAREA = 1 << 12;
    Object[][] data;

    volatile int readIndex = -1;
    volatile int writeIndex = -1;
    private final AtomicInteger count = new AtomicInteger();
    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();
    private final ReentrantLock putLock = new ReentrantLock();
    private final Condition notFull = putLock.newCondition();

    private void signalNotEmpty() {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    private void signalNotFull() {
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            notFull.signal();
        } finally {
            putLock.unlock();
        }
    }

    int capacity;
    int rowOffice;
    int colOffice;
    int rowSize;
    int bitHigh;
    int subareaSize;
    int maxSize;

    public RingBlockingQueue(int subareaSize, int capacity) {
        this(subareaSize, capacity, 1);
    }

    public RingBlockingQueue(int subareaSize, int capacity, int concurrency) {
        if (subareaSize > capacity || capacity < 0 || subareaSize < 0) {
            throw new IllegalArgumentException("Illegal initial capacity:subareaSize>capacity||capacity<0||subareaSize<0");
        }
        maxSize = capacity;
        subareaSize = subareaSizeFor(subareaSize);
        capacity = tableSizeFor(capacity);
        rowSize = tableSizeFor(capacity / subareaSize);
        capacity = rowSize * subareaSize;

        data = new Object[rowSize][subareaSize];
        this.capacity = capacity;
        bitHigh = getIntHigh(subareaSize);
        this.subareaSize = subareaSize;
        rowOffice = rowSize - 1;
        colOffice = subareaSize - 1;
    }

    public RingBlockingQueue(Collection<? extends E> c) {
        this(8888, 88888);
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            int n = 0;
            for (E e : c) {
                if (e == null) {
                    throw new NullPointerException();
                }
                if (n == capacity) {
                    throw new IllegalStateException("Queue full");
                }
                put(e);
                ++n;
            }
            count.set(n);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupted status
            throw new RuntimeException(e);
        } finally {
            putLock.unlock();
        }
    }

    static int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    static int getIntHigh(int cap) {
        int high = 0;
        while ((cap & 1) == 0) {
            high++;
            cap = cap >> 1;
        }
        return high;
    }

    static int subareaSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_SUBAREA) ? MAXIMUM_SUBAREA : n + 1;
    }

    void refreshIndex() {
        if (readIndex > capacity) {
            putLock.lock();
            try {
                synchronized (this) {
                    if (readIndex > capacity) {
                        writeIndex -= capacity;
                        readIndex -= capacity;
                    }
                }
            } finally {
                putLock.unlock();
            }
        }
    }

    @Override
    public boolean offer(E o) {
        if (o == null) {
            throw new NullPointerException();
        }
        putLock.lock();
        try {
            if (count.get() >= capacity) {
                return false;
            }
            int localWriteIndex = writeIndex + 1;
            if (localWriteIndex > readIndex + maxSize) {
                return false;
            }
            count.incrementAndGet();
            writeIndex = localWriteIndex;
            int row = (localWriteIndex >> bitHigh) & rowOffice;
            int column = localWriteIndex & colOffice;
            if (column == 0 && row == 0) {
                refreshIndex();
            }
            data[row][column] = o;
            return true;
        } finally {
            putLock.unlock();
        }
    }

    @Override
    public E poll() {
        takeLock.lock();
        try {
            if (writeIndex <= readIndex) {
                return null;
            }
            int localReadIndex = readIndex + 1;
            readIndex = localReadIndex;
            int row = (localReadIndex >> bitHigh) & rowOffice;
            int column = localReadIndex & colOffice;
            if (column == 0 && row == 0) {
                refreshIndex();
            }
            E result = (E) data[row][column];
            count.decrementAndGet();
            return result;
        } finally {
            takeLock.unlock();
        }
    }

    E ergodic(Integer index) {
        if (index > writeIndex || index < readIndex) {
            return null;
        }
        int row = (index >> bitHigh) & rowOffice;
        int column = index & colOffice;
        if (column == 0 && row == 0) {
            refreshIndex();
        }
        return (E) data[row][column];
    }

    @Override
    public E peek() {
        takeLock.lock();
        try {
            if (writeIndex <= readIndex) {
                return null;
            }
            int localReadIndex = readIndex;
            int row = (localReadIndex >> bitHigh) & rowOffice;
            int column = localReadIndex & colOffice;
            if (column == 0 && row == 0) {
                refreshIndex();
            }
            return (E) data[row][column];
        } finally {
            takeLock.unlock();
        }
    }

    @Override
    public void put(E o) throws InterruptedException {
        if (o == null) {
            throw new NullPointerException();
        }
        int c = -1;
        final ReentrantLock putLock = this.putLock;
        final AtomicInteger count = this.count;
        putLock.lockInterruptibly();
        try {
            while (count.get() == capacity) {
                notFull.await();
            }
            offer(o);
            c = count.get();
            if (c + 1 < capacity) {
                notFull.signal();
            }
        } finally {
            putLock.unlock();
        }
        if (c == 0) {
            signalNotEmpty();
        }
    }

    @Override
    public boolean offer(E o, long timeout, TimeUnit unit) throws InterruptedException {
        if (o == null) {
            throw new NullPointerException();
        }
        long nanos = unit.toNanos(timeout);
        int c = -1;
        final ReentrantLock putLock = this.putLock;
        final AtomicInteger count = this.count;
        putLock.lockInterruptibly();
        try {
            while (count.get() == capacity) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            offer(o);
            c = count.get();
            if (c + 1 < capacity) {
                notFull.signal();
            }
        } finally {
            putLock.unlock();
        }
        if (c == 0) {
            signalNotEmpty();
        }
        return true;
    }

    @Override
    public E take() throws InterruptedException {
        E x;
        int c = -1;
        final AtomicInteger count = this.count;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            while (count.get() == 0) {
                notEmpty.await();
            }
            x = poll();
            c = count.get();
            if (c > 1) {
                notEmpty.signal();
            }
        } finally {
            takeLock.unlock();
        }
        if (c == capacity) {
            signalNotFull();
        }
        return x;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E x = null;
        int c = -1;
        long nanos = unit.toNanos(timeout);
        final AtomicInteger count = this.count;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            while (count.get() == 0) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            x = poll();
            c = count.get();
            if (c > 1) {
                notEmpty.signal();
            }
        } finally {
            takeLock.unlock();
        }
        if (c == capacity) {
            signalNotFull();
        }
        return x;
    }

    @Override
    public int remainingCapacity() {
        return capacity - count.get();
    }

    @Override
    public boolean remove(Object o) {
        fullyLock();
        try {
            for (int index = readIndex; readIndex >= index || index <= writeIndex; index++) {
                if (o.equals(ergodic(index))) {
                    // Remove the element and shift the rest
                    for (int i = index; i < writeIndex; i++) {
                        ergodic(i).equals(ergodic(i + 1));
                    }
                    writeIndex--;
                    count.decrementAndGet();
                    return true;
                }
            }
            return false;
        } finally {
            fullyUnlock();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RingBlockingQueue<?> that = (RingBlockingQueue<?>) o;
        return Arrays.deepEquals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(data);
    }

    @Override
    public boolean retainAll(Collection c) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean removeAll(Collection c) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean containsAll(Collection c) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int size() {
        return count.get();
    }

    @Override
    public boolean isEmpty() {
        return count.get() == 0;
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) {
            return false;
        }
        fullyLock();
        try {
            for (int index = readIndex; readIndex >= index || index <= writeIndex; index++) {
                if (o.equals(ergodic(index))) {
                    return true;
                }
            }
            return false;
        } finally {
            fullyUnlock();
        }
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {
            private int currentIndex = readIndex;
            private final int expectedModCount = count.get();

            @Override
            public boolean hasNext() {
                return currentIndex < writeIndex;
            }

            @Override
            public E next() {
                if (currentIndex >= writeIndex) {
                    throw new NoSuchElementException();
                }
                if (expectedModCount != count.get()) {
                    throw new ConcurrentModificationException();
                }
                return (E) ergodic(currentIndex++);
            }
        };
    }

    void fullyLock() {
        putLock.lock();
        takeLock.lock();
    }

    void fullyUnlock() {
        takeLock.unlock();
        putLock.unlock();
    }

    @Override
    public Object[] toArray() {
        fullyLock();
        try {
            int size = count.get();
            Object[] a = new Object[size];
            int k = 0;
            for (int index = readIndex; readIndex >= index || index <= writeIndex; index++) {
                a[k++] = ergodic(index);
            }
            return a;
        } finally {
            fullyUnlock();
        }
    }

    @Override
    public <T> T[] toArray(T[] a) {
        fullyLock();
        try {
            int size = count.get();
            if (a.length < size) {
                a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
            }

            int k = 0;
            for (int index = readIndex; readIndex >= index || index <= writeIndex; index++) {
                a[k++] = (T) ergodic(index);
            }
            if (a.length > k) {
                a[k] = null;
            }
            return a;
        } finally {
            fullyUnlock();
        }
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        if (c == null) {
            throw new NullPointerException();
        }
        if (c == this) {
            throw new IllegalArgumentException();
        }
        if (maxElements <= 0) {
            return 0;
        }
        boolean signalNotFull = false;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            int n = Math.min(maxElements, count.get());
            E e;
            int i = 0;
            try {
                while (i < n && (e = poll()) != null) {
                    c.add(e);
                    i++;
                }
                return i;
            } finally {
                if (i > 0) {
                    signalNotFull = (count.getAndAdd(-i) == capacity);
                }
            }
        } finally {
            takeLock.unlock();
            if (signalNotFull) {
                signalNotFull();
            }
        }
    }

    @Override
    public boolean add(E e) {
        if (offer(e)) {
            return true;
        } else {
            throw new IllegalStateException("Queue full");
        }
    }

    @Override
    public E remove() {
        E x = poll();
        if (x != null) {
            return x;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public E element() {
        E x = peek();
        if (x != null) {
            return x;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void clear() {
        while (poll() != null) {
            ;
        }
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        if (c == null) {
            throw new NullPointerException();
        }
        if (c == this) {
            throw new IllegalArgumentException();
        }
        boolean modified = false;
        for (E e : c) {
            if (add(e)) {
                modified = true;
            }
        }
        return modified;
    }
}
