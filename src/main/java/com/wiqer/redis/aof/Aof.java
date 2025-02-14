package com.wiqer.redis.aof;

import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.aof.queue.RingBlockingQueue;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.command.WriteCommandFactory;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.util.Format;
import com.wiqer.redis.util.PropertiesUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Redis AOF持久化实现类
 * 通过记录所有的写操作命令来实现数据持久化
 * 特点：
 * 1. 使用分段存储，每个文件限制大小为64MB
 * 2. 使用内存映射提高I/O性能
 * 3. 支持命令的批量写入和读取
 * 4. 采用单线程处理确保命令顺序
 *
 * @author lilan
 */
@Slf4j
public class Aof {

    private static final String SUFFIX = ".aof";
    private static final int SHIFT_BIT = 26;
    private static final int ONE_PIECE = 1 << SHIFT_BIT;
    private static final int INITIAL_BUFFER_SIZE = 8888;
    private static final int MAX_BUFFER_SIZE = 2147483647;
    private static final int QUEUE_INITIAL_CAPACITY = 8888;
    private static final int QUEUE_MAX_CAPACITY = 888888;
    private static final long CLEANUP_TIMEOUT_SECONDS = 5;

    private final AtomicLong aofPutIndex = new AtomicLong(0L);
    private final String fileName;
    private final RingBlockingQueue<List<Resp>> runtimeRespQueue;
    private final ByteBuf bufferPolled;
    private final ScheduledThreadPoolExecutor persistenceExecutor;

    @Getter
    private final RedisCore redisCore;
    private final ReadWriteLock reentrantLock = new ReentrantReadWriteLock();
    private volatile boolean isRunning = true;

    public Aof(RedisCore redisCore) {
        this.redisCore = redisCore;
        this.fileName = PropertiesUtil.getAofPath();
        this.runtimeRespQueue = new RingBlockingQueue<>(QUEUE_INITIAL_CAPACITY, QUEUE_MAX_CAPACITY);
        this.bufferPolled = PooledByteBufAllocator.DEFAULT.buffer(INITIAL_BUFFER_SIZE, MAX_BUFFER_SIZE);
        this.persistenceExecutor = createExecutor();
        initializeDirectory();
        start();
    }

    private ScheduledThreadPoolExecutor createExecutor() {
        return new ScheduledThreadPoolExecutor(1, r -> {
            Thread thread = new Thread(r, "Aof_Single_Thread");
            thread.setUncaughtExceptionHandler((t, e) ->
                    log.error("Uncaught exception in AOF thread: ", e));
            return thread;
        });
    }

    private void initializeDirectory() {
        File file = new File(this.fileName + SUFFIX);
        if (!file.isDirectory()) {
            File parentFile = file.getParentFile();
            if (parentFile != null && !parentFile.exists()) {
                if (!parentFile.mkdirs()) {
                    log.error("Failed to create directory: {}", parentFile.getAbsolutePath());
                }
            }
        }
    }

    public void put(Resp resp) {
        runtimeRespQueue.offer(List.of(resp));
    }

    public void put(List<Resp> resp) {
        runtimeRespQueue.offer(resp);
    }

    /**
     * 启动数据持久化和磁盘数据同步任务
     * <p>
     * 本方法负责启动两个任务：一是从磁盘加载所有段的数据到内存，二是定期将所有段的数据从内存同步到磁盘
     * 使用persistenceExecutor执行器来管理这些任务，确保它们可以异步执行，提高系统的响应速度和处理能力
     */
    public void start() {
        // 加载磁盘上所有段的数据到内存中，启动时执行一次
        persistenceExecutor.execute(this::pickupDiskDataAllSegment);

        // 定期将所有段的数据从内存同步到磁盘上，首次执行延迟10秒后进行，之后每隔1秒执行一次
        persistenceExecutor.scheduleAtFixedRate(this::downDiskAllSegment, 10, 1, TimeUnit.SECONDS);
    }

    public void close() {
        isRunning = false;
        try {
            persistenceExecutor.shutdown();
            if (!persistenceExecutor.awaitTermination(CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                persistenceExecutor.shutdownNow();
                if (!persistenceExecutor.awaitTermination(CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    log.error("AOF executor did not terminate");
                }
            }
            if (bufferPolled != null && bufferPolled.refCnt() > 0) {
                bufferPolled.release();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while closing AOF", e);
        }
    }

    /**
     * 将内存中的所有数据段持久化到磁盘
     * 该方法主要用于将Redis响应队列中的数据异步写入到持久化文件中
     */
    public void downDiskAllSegment() {
        if (!isRunning || !reentrantLock.writeLock().tryLock()) {
            return;
        }
        try {
            processSegments();
        } catch (Exception e) {
            log.error("Error during disk write operation", e);
        } finally {
            reentrantLock.writeLock().unlock();
        }
    }

    private void processSegments() {
        long segmentId = -1;
        // 遍历所有数据段，直到当前数据段与待写入索引指示的数据段一致
        Segment:
        while (segmentId != (aofPutIndex.get() >> SHIFT_BIT)) {
            segmentId = (aofPutIndex.get() >> SHIFT_BIT);
            // 打开或创建一个文件通道，用于读写指定的数据段文件
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(fileName + "_" + segmentId + SUFFIX, "rw");
                 FileChannel channel = randomAccessFile.getChannel()) {
                long len = channel.size();
                int putIndex = Format.uintNBit(aofPutIndex.get(), SHIFT_BIT);
                long baseOffset = aofPutIndex.get() - putIndex;

                // 调整数据段长度，确保有足够的空间写入数据
                if (len - putIndex < 1L << (SHIFT_BIT - 2)) {
                    len = segmentId + 1 << (SHIFT_BIT - 2);
                }

                MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, len);

                // 循环处理响应队列中的数据，直到队列为空或数据写满当前数据段
                do {
                    List<Resp> list = runtimeRespQueue.peek();
                    if (list == null || list.isEmpty()) {
                        clean(mappedByteBuffer);
                        break Segment;
                    }

                    // 将响应数据序列化到缓冲区，并判断当前数据段是否有足够的空间写入
                    Resp.write(list, bufferPolled);
                    int respLen = bufferPolled.readableBytes();

                    if ((mappedByteBuffer.capacity() <= respLen + putIndex)) {
                        len += 1L << (SHIFT_BIT - 3);
                        if (len > ONE_PIECE) {
                            bufferPolled.release();
                            aofPutIndex.set(baseOffset + ONE_PIECE);
                            break;
                        }
                        mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, len);
                    }

                    // 将缓冲区中的数据写入到数据段文件中
                    while (respLen > 0) {
                        respLen--;
                        mappedByteBuffer.put(putIndex++, bufferPolled.readByte());
                    }

                    aofPutIndex.set(baseOffset + putIndex);
                    runtimeRespQueue.poll();
                    bufferPolled.clear();

                    // 根据需要扩展数据段长度，如果超出最大长度则进行清理并跳出循环
                    if (len - putIndex < (1L << (SHIFT_BIT - 3))) {
                        len += 1L << (SHIFT_BIT - 3);
                        if (len > ONE_PIECE) {
                            bufferPolled.release();
                            clean(mappedByteBuffer);
                            aofPutIndex.set(baseOffset + ONE_PIECE);
                            break;
                        }
                        mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, len);
                    }
                } while (true);
            } catch (IOException e) {
                log.error("IO Exception in downDiskAllSegment", e);
            }
        }
    }

    /**
     * 从所有段中获取磁盘数据
     * 该方法通过读取和处理每个段的AOF文件来恢复数据
     */
    public void pickupDiskDataAllSegment() {
        // 获取写锁以确保线程安全
        reentrantLock.writeLock().lock();
        try {
            long segmentId = -1;
            // 循环读取直到追上当前的AOF索引
            Segment:
            while (segmentId != (aofPutIndex.get() >> SHIFT_BIT)) {
                segmentId = (aofPutIndex.get() >> SHIFT_BIT);
                int putIndex = Format.uintNBit(aofPutIndex.get(), SHIFT_BIT);
                long baseOffset = aofPutIndex.get() - putIndex;

                // 打开并映射AOF文件到内存
                try (RandomAccessFile randomAccessFile = new RandomAccessFile(fileName + "_" + segmentId + SUFFIX, "r");
                     FileChannel channel = randomAccessFile.getChannel()) {
                    long len = channel.size();
                    MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, len);

                    // 分配缓冲区以读取AOF文件内容
                    ByteBuf bufferPolled = PooledByteBufAllocator.DEFAULT.buffer((int) len);
                    try {
                        bufferPolled.writeBytes(mappedByteBuffer);

                        // 循环处理AOF文件中的命令
                        do {
                            try {
                                List<Resp> list = Resp.decode(bufferPolled);
                                WriteCommand command = WriteCommandFactory.create(redisCore).from(list);
                                command.handle();
                            } catch (Exception e) {
                                break Segment;
                            }

                            // 更新读取索引
                            putIndex = bufferPolled.readerIndex();
                            aofPutIndex.set(putIndex + baseOffset);

                            // 如果读取索引超过一段的大小，则更新aofPutIndex并中断循环
                            if (putIndex > ONE_PIECE) {
                                aofPutIndex.set(baseOffset + ONE_PIECE);
                                break;
                            }
                        } while (true);
                    } finally {
                        // 释放缓冲区并清理内存映射文件
                        bufferPolled.release();
                        clean(mappedByteBuffer);
                    }
                } catch (IOException e) {
                    log.error("Failed to process AOF file for segment {}", segmentId, e);
                    break;
                }
            }
        } catch (Exception e) {
            log.error("Failed to pickup disk data from all segments", e);
        } finally {
            // 释放写锁
            reentrantLock.writeLock().unlock();
        }
    }

    public static void clean(final MappedByteBuffer buffer) {
        if (buffer == null) {
            return;
        }
        buffer.force();
        try {
            Method cleanerMethod = buffer.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(buffer);
            Method cleanMethod = cleaner.getClass().getMethod("clean");
            cleanMethod.invoke(cleaner);
        } catch (Exception e) {
            log.error("Unexpected error during cleanup: ", e);
        }
    }
}
