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
import java.io.RandomAccessFile;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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

    /**
     * AOF文件后缀
     */
    private static final String suffix = ".aof";

    /**
     * 分片大小控制位
     * 1. 经过大量测试，使用过3年以上机械磁盘的最大性能为26
     * 2. 存盘偏移量，控制单个持久化文件大小
     * 3. 1 << 26 约等于 64MB
     */
    private static final int shiftBit = 26;
    /**
     * 单个分片的大小：64MB
     */
    private static final int onePiece = 1 << shiftBit;

    /**
     * 当前AOF写入位置索引，用于追踪写入进度
     */
    private Long aofPutIndex = 0L;

    /**
     * AOF文件路径，从配置文件中读取
     */
    private final String fileName = PropertiesUtil.getAofPath();

    /**
     * 运行时命令队列
     * 初始容量：8888
     * 最大容量：888888
     * 用于临时存储待写入磁盘的命令
     */
    private final RingBlockingQueue<List<Resp>> runtimeRespQueue =
            new RingBlockingQueue<>(8888, 888888);

    /**
     * 用于数据序列化的缓冲区
     * 初始容量：8888
     * 最大容量：2GB
     */
    private final ByteBuf bufferPolled =
            new PooledByteBufAllocator().buffer(8888, 2147483647);

    /**
     * AOF持久化专用线程池
     * 使用单线程确保命令顺序写入
     * 线程名称前缀：Aof_Single_Thread
     */
    private final ScheduledThreadPoolExecutor persistenceExecutor =
            new ScheduledThreadPoolExecutor(1,
                    r -> new Thread(r, "Aof_Single_Thread"));

    /**
     * Redis核心实例引用，用于执行命令重放
     */
    @Getter
    private final RedisCore redisCore;

    /**
     * 读写锁
     * 用于协调AOF文件的读写访问
     * 保证文件操作的线程安全
     */
    final ReadWriteLock reentrantLock = new ReentrantReadWriteLock();

    /**
     * 构造函数
     * 1. 初始化Redis核心实例
     * 2. 确保AOF文件目录存在
     * 3. 启动AOF服务
     *
     * @param redisCore Redis核心实例
     */
    public Aof(RedisCore redisCore) {
        this.redisCore = redisCore;
        // 确保AOF文件目录存在
        File file = new File(this.fileName + suffix);
        if (!file.isDirectory()) {
            File parentFile = file.getParentFile();
            if (null != parentFile && !parentFile.exists()) {
                parentFile.mkdirs();
            }
        }
        start();
    }

    /**
     * 将单个命令添加到AOF队列
     *
     * @param resp Redis命令响应对象
     */
    public void put(Resp resp) {
        runtimeRespQueue.offer(List.of(resp));
    }

    /**
     * 将多个命令批量添加到AOF队列
     *
     * @param resp Redis命令响应对象列表
     */
    public void put(List<Resp> resp) {
        runtimeRespQueue.offer(resp);
    }

    /**
     * 启动AOF服务
     * 1. 首先从磁盘加载已有的AOF文件
     * 2. 启动定期持久化任务（每秒执行一次）
     */
    public void start() {
        persistenceExecutor.execute(this::pickupDiskDataAllSegment);
        persistenceExecutor.scheduleAtFixedRate(this::downDiskAllSegment,
                10, 1, TimeUnit.SECONDS);
    }

    /**
     * 优雅关闭AOF服务
     * 关闭持久化线程池
     */
    public void close() {
        try {
            persistenceExecutor.shutdown();
        } catch (Exception exp) {
            log.warn("Exception!", exp);
        }
    }

    /**
     * 将内存中的命令持久化到磁盘
     * 采用分段存储策略，每个文件大小不超过 2^shiftBit
     * 处理流程：
     * 1. 获取写锁
     * 2. 计算当前段ID
     * 3. 创建或打开对应的文件
     * 4. 使用内存映射写入数据
     * 5. 根据需要扩展文件大小
     */
    public void downDiskAllSegment() {
        if (reentrantLock.writeLock().tryLock()) {
            try {
                long segmentId = -1;
                Segment:
                while (segmentId != (aofPutIndex >> shiftBit)) {
                    // 计算当前段ID
                    segmentId = (aofPutIndex >> shiftBit);
                    // 打开对应的AOF文件
                    RandomAccessFile randomAccessFile = new RandomAccessFile(fileName + "_" + segmentId + suffix, "rw");
                    FileChannel channel = randomAccessFile.getChannel();
                    long len = channel.size();
                    // 计算段内偏移量
                    int putIndex = Format.uintNBit(aofPutIndex, shiftBit);
                    long baseOffset = aofPutIndex - putIndex;

                    // 如果文件剩余空间不足，扩展文件大小
                    if (len - putIndex < 1L << (shiftBit - 2)) {
                        len = segmentId + 1 << (shiftBit - 2);
                    }

                    // 创建内存映射
                    MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, len);

                    do {
                        // 获取待写入的命令
                        List<Resp> list = runtimeRespQueue.peek();
                        if (list == null || list.isEmpty()) {
                            clean(mappedByteBuffer);
                            randomAccessFile.close();
                            break Segment;
                        }

                        // 序列化命令
                        Resp.write(list, bufferPolled);
                        int respLen = bufferPolled.readableBytes();

                        // 检查并扩展文件容量
                        if ((mappedByteBuffer.capacity() <= respLen + putIndex)) {
                            len += 1L << (shiftBit - 3);
                            mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, len);
                            if (len > (1 << shiftBit)) {
                                bufferPolled.release();
                                aofPutIndex = baseOffset + 1 << shiftBit;
                                break;
                            }
                        }

                        // 写入数据
                        while (respLen > 0) {
                            respLen--;
                            mappedByteBuffer.put(putIndex++, bufferPolled.readByte());
                        }

                        // 更新索引并清理
                        aofPutIndex = baseOffset + putIndex;
                        runtimeRespQueue.poll();
                        bufferPolled.clear();

                        // 检查是否需要扩展文件
                        if (len - putIndex < (1L << (shiftBit - 3))) {
                            len += 1L << (shiftBit - 3);
                            if (len > (1 << shiftBit)) {
                                bufferPolled.release();
                                clean(mappedByteBuffer);
                                aofPutIndex = baseOffset + 1 << shiftBit;
                                break;
                            }
                            mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, len);
                        }
                    } while (true);
                }
            } catch (Exception e) {
                log.error("aof Exception", e);
            } finally {
                reentrantLock.writeLock().unlock();
            }
        }
    }

    /**
     * 从磁盘加载AOF文件并重放命令
     * 采用分段读取策略，保证内存使用效率
     * 处理流程：
     * 1. 获取写锁
     * 2. 按段读取文件
     * 3. 解析命令并重放
     * 4. 更新索引位置
     */
    public void pickupDiskDataAllSegment() {
        if (reentrantLock.writeLock().tryLock()) {
            try {
                long segmentId = -1;
                Segment:
                while (segmentId != (aofPutIndex >> shiftBit)) {
                    segmentId = (aofPutIndex >> shiftBit);
                    RandomAccessFile randomAccessFile = new RandomAccessFile(fileName + "_" + segmentId + suffix, "r");
                    FileChannel channel = randomAccessFile.getChannel();
                    long len = channel.size();
                    int putIndex = Format.uintNBit(aofPutIndex, shiftBit);
                    long baseOffset = aofPutIndex - putIndex;

                    // 创建内存映射和缓冲区
                    MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, len);
                    ByteBuf bufferPolled = new PooledByteBufAllocator().buffer((int) len);
                    bufferPolled.writeBytes(mappedByteBuffer);

                    do {
                        // 解析命令
                        List<Resp> list;
                        try {
                            list = Resp.decode(bufferPolled);
                        } catch (Exception e) {
                            clean(mappedByteBuffer);
                            randomAccessFile.close();
                            bufferPolled.release();
                            break Segment;
                        }

                        // 执行命令
                        WriteCommand command = WriteCommandFactory.create(redisCore).from(list);
                        command.handle();

                        // 更新索引
                        putIndex = bufferPolled.readerIndex();
                        aofPutIndex = putIndex + baseOffset;

                        // 检查是否需要切换到下一个段
                        if (putIndex > onePiece) {
                            bufferPolled.release();
                            clean(mappedByteBuffer);
                            aofPutIndex = baseOffset + onePiece;
                            break;
                        }
                    } while (true);
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            } finally {
                reentrantLock.writeLock().unlock();
            }
        }
    }

    /**
     * 清理MappedByteBuffer资源
     * 通过反射调用cleaner方法强制释放内存映射
     * 这是必要的，因为MappedByteBuffer不会被GC自动回收
     *
     * @param buffer 需要清理的MappedByteBuffer
     */
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

    /**
     * 测试方法
     * 打印单个分片的大小（64MB）
     */
    public static void main(String[] args) {
        System.out.println(1 << shiftBit);
    }
}
