/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * MappedFile可以看做是每一个Commitlog文件的映射，里面记录了文件的大小以及数据已经写入的位置，还有两个字节缓冲区ByteBuffer和MappedByteBuffer，
 * MappedFile提供了两种方式来进行内容的写入：
 *  第一种通过ByteBuffer分配缓冲区并将内容写入缓冲区，并且使用了暂存池对内存进行管理，需要时进行申请，使用完毕后回收，类似于数据库连接池。
 *  第二种是通过MappedByteBuffer，对CommitLog进行文件映射，然后进行消息写入。
 * 综上所述，开启使用暂存池时会使用ByteBuffer，否则使用MappedByteBuffer进行内容写入。
 *
 * 消息内容已经写入到内存缓冲区中，并且也知道准备进行写入的CommitLog对应的映射文件，接下来就可以调用MappedFile的appendMessagesInner
 * 方法将内存中的内容写入映射文件，处理逻辑如下：
 *  1. MappedFile中记录了文件的写入位置，获取准备写入的位置，如果写入的位置小于文件大小，意味着当前文件可以进行内容写入，反之说明此文件已写满
 *     ，不能继续下一步，需要返回错误信息
 *  2. 如果writeBuffer不为空，使用writeBuffer，否则使用mappedByteBuffer的slice方法创建一个与MappedFile共享的内存区byteBuffer
 *     ，设置byteBuffer的写入位置，之后通过byteBuffer来进行消息写入，由于是共享内存区域，所以写入的内容会影响到writeBuffer或者mappedByteBuffer中
 *  3. 调用回调函数的doAppend方法进行写入，前面可知回调函数是DefaultAppendMessageCallback类型的
 *  4. 更新MappedFile写入位置，返回写入结果
 *
 **/

public class MappedFile extends ReferenceResource {
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     *记录文件的写入位置
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    /**
     * 数据提交位置
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    /**
     * 数据flush位置
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    /**
     * 文件大小
     */
    protected int fileSize;
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     * 字节缓冲区，用于在内存中分配空间，可以在JVM堆中分配内存（HeapByteBuffer），也可以在堆外分配内存（DirectByteBuffer）
     */
    protected ByteBuffer writeBuffer = null;

    /**
     * 是ByteBuffer的子类，它是将磁盘的文件内容映射到虚拟地址空间，通过虚拟地址访问物理内存中映射的文件内容，也叫文件映射，可以减少数据的拷贝。
     */
    private MappedByteBuffer mappedByteBuffer;
    /**
     * 暂存池，类似线程池，只不过池中存放的是申请的内存
     */
    protected TransientStorePool transientStorePool = null;
    private String fileName;
    private long fileFromOffset;
    private File file;
    private volatile long storeTimestamp = 0;
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     *  初始化
     * @param fileName
     * @param fileSize
     * @param transientStorePool
     * @throws IOException
     */
    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        // 从暂存池中获取一块内存
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            // 获取文件
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 进行文件映射
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * 写入消息
     * @param msg
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        // 调用appendMessagesInner
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;
        // 获取写入位置
        int currentPos = this.wrotePosition.get();
        // 如果写指针小于文件大小
        if (currentPos < this.fileSize) {
            // 如果writeBuffer不为空，使用writeBuffer的slice方法创建共享内存区，否则使用mappedByteBuffer
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            // 设置共享内存区的写入位置
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            // 单个消息处理
            if (messageExt instanceof MessageExtBrokerInner) {
                // 通过共享内存区byteBuffer写入数据
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                // 批量消息
                // 通过共享内存区byteBuffer写入数据
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            // 更新MappedFile的写入位置
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }


    /**
     * 进行刷盘并返回flush后的偏移量
     * flush的逻辑也与commit方法的逻辑类似：
     *      1. 调用isAbleToFlush判断是否满足刷盘条件，获取上次flush位置偏移量和当前写入位置偏移量进行如下校验：
     *          a. 文件是否已写满，即文件大小是否与写入数据位置相等，如果相等说明文件已经写满需要执行刷盘，满足刷盘条件
     *          b. 如果最少flush页数大于0，计算本次flush的页数是否大于或等于最少flush页数，如果满足可以进行刷盘
     *              本次flush数据的页数计算方法：写入位置/页大小 - flush位置/页大小
     *          c. 如果写入位置偏移量是否大于flush位置偏移量，如果大于表示有数据未进行刷盘，满足刷盘条件
     *      2. 调用fileChannel的force或者mappedByteBuffer的force方法进行刷盘
     *      3. 记录本次flush的位置，并作为结果返回
     * @param flushLeastPages
     * @return
     */
    public int flush(final int flushLeastPages) {
        // 是否可以刷盘
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    // 如果writeBuffer不为空
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        // 将数据刷到硬盘
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }
                // 记录flush位置
                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        // 返回flush位置
        return this.getFlushedPosition();
    }
    /**
     * MappedFile中记录CommitLog的写入位置wrotePosition、提交位置committedPosition以及flush位置flushedPosition，在commit方法中
     * ，调用了isAbleToCommit判断是否可以提交数据，判断的流程如下:
     *      1. 获取提交数据的位置偏移量和写入数据的位置偏移量
     *      2. 如果最少提交页数大于0，计算本次写入的页数是否大于或等于最少提交页数
     *          本次写入数据的页数计算方法：写入位置/页大小 - flush位置/页大小
     *      3. 如果以上条件都满足，判断写入位置是否大于flush位置，如果大于表示有一部数据未flush可以进行提交
     * 满足提交条件后，就会调用commit0方法提交数据，将数据写入到fileChannel中
     * @return The current flushed position
     */
    public int commit(final int commitLeastPages) {
        // 如果writeBuffer为空
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            // 不需要提交任何数据到，返回之前记录的写入位置
            return this.wrotePosition.get();
        }
        // 如果可以提交数据
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                // 提交数据
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }
        // 返回提交位置
        return this.committedPosition.get();
    }

    protected void commit0(final int commitLeastPages) {
        // 获取写入位置
        int writePos = this.wrotePosition.get();
        // 获取上次提交的位置
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - lastCommittedPosition > commitLeastPages) {
            try {
                // 创建共享缓冲区
                ByteBuffer byteBuffer = writeBuffer.slice();
                // 设置上一次提交位置
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                // 数据写入fileChannel
                this.fileChannel.write(byteBuffer);
                // 更新写入的位置
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 是否可以刷盘
     * @param flushLeastPages
     * @return
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        // 获取上次flush位置
        int flush = this.flushedPosition.get();
        // 写入位置偏移量
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }
        // 如果flush的页数大于0，校验本次flush的页数是否满足条件
        if (flushLeastPages > 0) {
            // 本次flush的页数：写入位置偏移量/OS_PAGE_SIZE - 上次flush位置偏移量/OS_PAGE_SIZE，是否大于flushLeastPages
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        // 写入位置偏移量是否大于flush位置偏移量
        return write > flush;
    }

    /**
     * 是否可以提交数据
     * @param commitLeastPages
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        // 获取提交数据的位置偏移量
        int flush = this.committedPosition.get();
        // 获取写入数据的位置偏移量
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }
        // 如果最少提交页数大于0
        if (commitLeastPages > 0) {
            // 写入位置/页大小 - flush位置/页大小 是否大于至少提交的页数
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }
        // 判断是否需要flush数据
        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    /**
     * 文件是否已写满
     * @return
     */
    public boolean isFull() {
        // 文件大小是否与写入数据位置相等
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * 返回当前有效数据的位置
     * @return The max position which have valid data
     *
     */
    public int getReadPosition() {
        // 如果writeBuffer为空使用写入位置，否则使用提交位置
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
