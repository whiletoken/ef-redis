package com.wiqer.redis.aof.queue;

import org.jetbrains.annotations.NotNull;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 环形阻塞队列实现
 * 特点：
 * 1. 使用二维数组存储，提高内存使用效率
 * 2. 支持阻塞和非阻塞操作
 * 3. 使用读写分离锁提高并发性能
 * 4. 使用位运算优化性能
 */
public class RingBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E>, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    // 队列最大容量（2^29）
    private static final int MAXIMUM_CAPACITY = 1 << 29;
    // 子区域最大大小（2^12 = 4096）
    private static final int MAXIMUM_SUBAREA = 1 << 12;
    // 队列最小容量
    private static final int MIN_CAPACITY = 16;

    // 存储数据的二维数组
    private final Object[][] data;
    // 队列总容量
    private final int capacity;
    // 行索引掩码（用于位运算）
    private final int rowOffice;
    // 列索引掩码（用于位运算）
    private final int colOffice;
    // 位运算使用的位数
    private final int bitHigh;
    // 最大可用大小
    private final int maxSize;

    // 读取位置索引（volatile保证可见性）
    final AtomicInteger readIndex = new AtomicInteger(-1);
    // 写入位置索引（volatile保证可见性）
    final AtomicInteger writeIndex = new AtomicInteger(-1);
    // 当前元素数量
    private final AtomicInteger count = new AtomicInteger();
    // 取元素锁
    private final ReentrantLock takeLock = new ReentrantLock();
    // 非空条件
    private final Condition notEmpty = takeLock.newCondition();
    // 放元素锁
    private final ReentrantLock putLock = new ReentrantLock();
    // 非满条件
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

    public RingBlockingQueue(int subareaSize, int capacity) {
        this(subareaSize, capacity, 1);
    }

    // 8888 88888
    public RingBlockingQueue(int subareaSize, int capacity, int concurrency) {
        if (capacity < MIN_CAPACITY) {
            capacity = MIN_CAPACITY;
        }
        if (subareaSize < MIN_CAPACITY) {
            subareaSize = MIN_CAPACITY;
        }
        if (subareaSize > capacity) {
            throw new IllegalArgumentException(
                    String.format("Illegal capacity: subareaSize(%d) > capacity(%d)", subareaSize, capacity)
            );
        }

        QueueInitializer initializer = new QueueInitializer(subareaSize, capacity);
        this.maxSize = initializer.maxSize;
        this.capacity = initializer.capacity;
        this.data = new Object[initializer.rowSize][initializer.colSize];
        this.bitHigh = getIntHigh(initializer.colSize);
        this.rowOffice = initializer.rowSize - 1;
        this.colOffice = initializer.colSize - 1;
    }

    /**
     * 队列初始化器
     * 用于计算和存储队列的初始化参数
     */
    private static class QueueInitializer {
        final int maxSize;
        final int capacity;
        final int colSize;
        final int rowSize;

        QueueInitializer(int subareaSize, int capacity) {
            this.maxSize = capacity;
            // 调整子区域大小为2的幂
            this.colSize = subareaSizeFor(subareaSize);
            // 计算需要的行数
            this.rowSize = tableSizeFor(tableSizeFor(capacity) / this.colSize);
            // 计算实际容量
            this.capacity = this.rowSize * this.colSize;
        }
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
        putLock.lock();
        try {
            if (readIndex.get() > capacity) {
                int offset = (readIndex.get() / capacity) * capacity;
                writeIndex.addAndGet(-offset);
                readIndex.addAndGet(-offset);
            }
        } finally {
            putLock.unlock();
        }
    }

    /**
     * 向队列中添加元素
     *
     * @param o 要添加的元素
     * @return 是否添加成功
     */
    @Override
    public boolean offer(E o) {
        // 不允许null元素
        Objects.requireNonNull(o, "Element cannot be null");

        putLock.lock();
        try {
            return tryOffer(o);
        } finally {
            putLock.unlock();
        }
    }

    /**
     * 尝试添加元素的内部方法
     *
     * @param o 要添加的元素
     * @return 是否添加成功
     */
    private boolean tryOffer(E o) {
        // 检查队列是否已满
        if (count.get() >= capacity) {
            return false;
        }

        // 计算新的写入位置
        int localWriteIndex = writeIndex.get() + 1;
        if (localWriteIndex > readIndex.get() + maxSize) {
            return false;
        }

        // 计算存储位置
        int row = calculateRow(localWriteIndex);
        int column = calculateColumn(localWriteIndex);

        writeIndex.set(localWriteIndex);
        count.incrementAndGet();

        // 检查是否需要刷新索引
        if (needsRefresh(row, column)) {
            refreshIndex();
        }

        // 存储元素
        data[row][column] = o;
        return true;
    }

    /**
     * 计算行索引
     *
     * @param index 全局索引
     * @return 行索引
     */
    private int calculateRow(int index) {
        return (index >> bitHigh) & rowOffice;
    }

    /**
     * 计算列索引
     *
     * @param index 全局索引
     * @return 列索引
     */
    private int calculateColumn(int index) {
        return index & colOffice;
    }

    /**
     * 检查是否需要刷新索引
     *
     * @param row    行索引
     * @param column 列索引
     * @return 是否需要刷新
     */
    private boolean needsRefresh(int row, int column) {
        return column == 0 && row == 0;
    }

    @Override
    public E poll() {
        takeLock.lock();
        try {
            if (writeIndex.get() <= readIndex.get()) {
                return null;
            }
            int localReadIndex = readIndex.get() + 1;
            readIndex.set(localReadIndex);
            int row = calculateRow(localReadIndex);
            int column = calculateColumn(localReadIndex);
            if (needsRefresh(row, column)) {
                refreshIndex();
            }
            E result = (E) data[row][column];
            if (result != null) {
                data[row][column] = null;
                count.decrementAndGet();
            }
            return result;
        } finally {
            takeLock.unlock();
        }
    }

    E ergodic(Integer index) {
        if (index > writeIndex.get() || index < readIndex.get()) {
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
            if (writeIndex.get() <= readIndex.get()) {
                return null;
            }
            int localReadIndex = readIndex.get();
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
    public @NotNull E take() throws InterruptedException {
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
        E x;
        int c;
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

    /**
     * 从队列中移除指定元素
     *
     * @param o 要移除的元素
     * @return 是否成功移除
     */
    @Override
    public boolean remove(Object o) {
        Objects.requireNonNull(o, "Element cannot be null");
        fullyLock();
        try {
            return tryRemove(o);
        } finally {
            fullyUnlock();
        }
    }

    /**
     * 尝试移除元素的内部方法
     *
     * @param o 要移除的元素
     * @return 是否成功移除
     */
    private boolean tryRemove(Object o) {
        int size = count.get();
        if (size == 0) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            int index = readIndex.get() + 1 + i;
            if (o.equals(ergodic(index))) {
                int currentRow = calculateRow(index);
                int currentCol = calculateColumn(index);
                int nextRow = calculateRow(index + 1);
                int nextCol = calculateColumn(index + 1);

                int remainingElements = writeIndex.get() - index;
                if (remainingElements > 0) {
                    if (currentRow == nextRow) {
                        System.arraycopy(data[currentRow], nextCol, data[currentRow], currentCol, remainingElements);
                    } else {
                        moveElementsAcrossRows(index, remainingElements);
                    }
                }
                int lastRow = calculateRow(writeIndex.get());
                int lastCol = calculateColumn(writeIndex.get());
                data[lastRow][lastCol] = null;

                writeIndex.getAndDecrement();
                count.decrementAndGet();
                return true;
            }
        }
        return false;
    }

    private void moveElementsAcrossRows(int startIndex, int count) {
        for (int i = 0; i < count; i++) {
            int sourceIndex = startIndex + i + 1;
            int targetIndex = startIndex + i;
            int sourceRow = calculateRow(sourceIndex);
            int sourceCol = calculateColumn(sourceIndex);
            int targetRow = calculateRow(targetIndex);
            int targetCol = calculateColumn(targetIndex);
            data[targetRow][targetCol] = data[sourceRow][sourceCol];
        }
    }

    /**
     * 移除指定位置的元素
     *
     * @param index 要移除的元素位置
     */
    private void removeAt(int index) {
        // 移动后续元素
        for (int i = index; i < writeIndex.get(); i++) {
            int nextRow = calculateRow(i + 1);
            int nextColumn = calculateColumn(i + 1);
            int currentRow = calculateRow(i);
            int currentColumn = calculateColumn(i);
            data[currentRow][currentColumn] = data[nextRow][nextColumn];
        }
        // 更新写入位置和元素计数
        writeIndex.getAndDecrement();
        count.decrementAndGet();
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
            for (int index = readIndex.get(); readIndex.get() >= index || index <= writeIndex.get(); index++) {
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
    public @NotNull Iterator<E> iterator() {
        return new Itr();
    }

    private class Itr implements Iterator<E> {
        private int cursor = readIndex.get() + 1;
        private int lastRet = -1;
        private final int fence = writeIndex.get() + 1;
        private final int expectedCount = count.get();

        @Override
        public boolean hasNext() {
            return cursor < fence;
        }

        @Override
        public E next() {
            if (expectedCount != count.get())
                throw new ConcurrentModificationException();
            if (cursor >= fence)
                throw new NoSuchElementException();

            lastRet = cursor;
            E result = ergodic(cursor++);
            return result;
        }

        @Override
        public void remove() {
            if (lastRet < 0)
                throw new IllegalStateException();
            if (expectedCount != count.get())
                throw new ConcurrentModificationException();

            RingBlockingQueue.this.removeAt(lastRet);
            cursor = lastRet;
            lastRet = -1;
        }
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
    public Object @NotNull [] toArray() {
        fullyLock();
        try {
            int size = count.get();
            Object[] a = new Object[size];
            int k = 0;
            for (int index = readIndex.get(); readIndex.get() >= index || index <= writeIndex.get(); index++) {
                a[k++] = ergodic(index);
            }
            return a;
        } finally {
            fullyUnlock();
        }
    }

    @Override
    public <T> T @NotNull [] toArray(T[] a) {
        fullyLock();
        try {
            int size = count.get();
            if (a.length < size) {
                a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
            }

            int k = 0;
            for (int index = readIndex.get(); readIndex.get() >= index || index <= writeIndex.get(); index++) {
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
