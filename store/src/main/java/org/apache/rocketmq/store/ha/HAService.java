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
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageStatus;

/**
 * 主从同步的实现逻辑主要在HAService中，在 DefaultMessageStore 的构造函数中，对HAService进行了实例化，并在start方法中，启动了HAService.
 */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AtomicInteger connectionCount = new AtomicInteger(0);

    private final List<HAConnection> connectionList = new LinkedList<>();

    private final AcceptSocketService acceptSocketService;

    private final DefaultMessageStore defaultMessageStore;

    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    /**
     * 向从节点推送的消息最大偏移量
     */
    private final GroupTransferService groupTransferService;

    private final HAClient haClient;

    /**
     * 1. 在HAService的构造函数中，创建了AcceptSocketService、GroupTransferService和HAClient，在start方法中主要做了如下几件事:
     *  1. 调用AcceptSocketService的beginAccept方法，这一步主要是进行端口绑定，在端口上监听从节点的连接请求（可以看做是运行在master节点的）;
     *  2. 调用AcceptSocketService的start方法启动服务，这一步主要为了处理从节点的连接请求，与从节点建立连接（可以看做是运行在master节点的）;
     *  3. 调用GroupTransferService的start方法，主要用于在主从同步的时候，等待数据传输完毕（可以看做是运行在master节点的）;
     *  4. 调用HAClient的start方法启动，里面与master节点建立连接，向master汇报主从同步进度并存储master发送过来的同步数据（可以看做是运行在从节点的）;
     *
     *
     * @param defaultMessageStore
     * @throws IOException
     */
    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        // 创建AcceptSocketService
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        // 创建HAClient
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * 面在GroupTransferService中可以看到是通过push2SlaveMaxOffset的值判断本次同步是否完成的，在notifyTransferSome方法中可以看到当Master
     * 节点收到从节点反馈的消息拉取偏移量时，对push2SlaveMaxOffset的值进行了更新：
     *
     * @param offset
     */
    public void notifyTransferSome(final long offset) {
        // 如果传入的偏移大于push2SlaveMaxOffset记录的值，进行更新
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            // 更新向从节点推送的消息最大偏移量
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    /**
     *1. 在start方法中主要做了如下几件事:
     *  1. 调用AcceptSocketService的beginAccept方法，这一步主要是进行端口绑定，在端口上监听从节点的连接请求（可以看做是运行在master节点的）;
     *  2. 调用AcceptSocketService的start方法启动服务，这一步主要为了处理从节点的连接请求，与从节点建立连接（可以看做是运行在master节点的）;
     *  3. 调用GroupTransferService的start方法，主要用于在主从同步的时候，等待数据传输完毕（可以看做是运行在master节点的）;
     *  4. 调用HAClient的start方法启动，里面与master节点建立连接，向master汇报主从同步进度并存储master发送过来的同步数据(处理master发送过来的数据)（可以看做是运行在从节点的）;
     *
     * @throws Exception
     */
    public void start() throws Exception {
        // 开始监听从服务器的连接
        this.acceptSocketService.beginAccept();
        // 启动服务
        this.acceptSocketService.start();
        // 启动GroupTransferService
        this.groupTransferService.start();
        // 启动
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {
        private final SocketAddress socketAddressListen;
        private ServerSocketChannel serverSocketChannel;
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *AcceptSocketService的beginAccept方法里面首先获取了ServerSocketChannel，然后进行端口绑定，并在selector上面注册了OP_ACCEPT事件的监听，监听从节点的连接请求。
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            // 创建ServerSocketChannel
            this.serverSocketChannel = ServerSocketChannel.open();
            // 获取selector
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            // 绑定端口
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            // 设置非阻塞
            this.serverSocketChannel.configureBlocking(false);
            // 注册OP_ACCEPT连接事件的监听
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         *处理从节点连接请求
         * 1. AcceptSocketService的run方法中，对监听到的连接请求进行了处理，处理逻辑大致如下：
         *  1. 从selector中获取到监听到的事件；
         *  2. 如果是OP_ACCEPT连接事件，创建与从节点的连接对象HAConnection，与从节点建立连接，然后调用HAConnection的start方法进行启动
         *  ，并创建的HAConnection对象加入到连接集合中，HAConnection中封装了Master节点和从节点的数据同步逻辑；
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            // 如果服务未停止
            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    // 获取监听到的事件
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    // 处理事件
                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            // 如果是连接事件
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        // 创建HAConnection，建立连接
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        // 启动
                                        conn.start();
                                        // 添加连接
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService Service
     * 等待主从复制传输结束
     * GroupTransferService的run方法主要是为了在进行主从数据同步的时候，等待从节点数据同步完毕。
     *
     * 在运行时首先进会调用waitForRunning进行等待，因为此时可能还有没有开始主从同步，所以先进行等待，之后如果有同步请求，会唤醒该线程
     * ，然后调用doWaitTransfer方法等待数据同步完成：
     *
     *
     *
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();

        /**
         * CommitLog提交请求集合
         */
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            this.wakeup();
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        /**
         * 1. 在看doWaitTransfer方法之前，首先看下是如何判断有数据需要同步的?
         *  a. Master节点中，当消息被写入到CommitLog以后，会调用 submitReplicaRequest 方法处主从同步，首先判断当前Broker的角色是否是SYNC_MASTER
         *     ，如果是则会构建消息提交请求GroupCommitRequest，然后调用HAService的putRequest添加到请求集合中，并唤醒GroupTransferService中在等待的线程.
         *
         * 2. 在doWaitTransfer方法中，会判断CommitLog提交请求集合requestsRead是否为空，如果不为空，表示有消息写入了CommitLog，Master节点需要等待将数据传输给从节点：
         *  a. push2SlaveMaxOffset记录了从节点已经同步的消息偏移量，判断push2SlaveMaxOffset是否大于本次CommitLog提交的偏移量，也就是请求中设置的偏移量；
         *  b. 获取请求中设置的等待截止时间；
         *  c. 开启循环，判断数据是否还未传输完毕，并且未超过截止时间，如果是则等待1s，然后继续判断传输是否完毕，不断进行，直到超过截止时间或者数据已经传输完毕；
         *      （向从节点发送的消息最大偏移量push2SlaveMaxOffset超过了请求中设置的偏移量表示本次同步数据传输完毕）；
         *  d. 唤醒在等待数据同步完毕的线程；
         *
         *
         */
        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                // 如果CommitLog提交请求集合不为空
                if (!this.requestsRead.isEmpty()) {
                    // 处理消息提交请求
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        // 判断传输到从节点最大偏移量是否超过了请求中设置的偏移量
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        long waitUntilWhen = HAService.this.defaultMessageStore.getSystemClock().now()
                            + HAService.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();
                        // 如果从节点还未同步完毕并且未超过截止时间
                        while (!transferOK && HAService.this.defaultMessageStore.getSystemClock().now() < waitUntilWhen) {
                            // 等待
                            this.notifyTransferObject.waitForRunning(1000);
                            // 判断从节点同步的最大偏移量是否超过了请求中设置的偏移量
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        // 唤醒
                        req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            // 如果服务未停止
            while (!this.isStopped()) {
                try {
                    // 等待运行
                    this.waitForRunning(10);
                    // 如果被唤醒，调用doWaitTransfer等待主从同步完成
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * 启动HAClient
     * 1. HAClient可以看做是在从节点上运行的，主要进行的处理如下：
     *    a. 调用connectMaster方法连接Master节点，Master节点上也会运行，但是它本身就是Master没有可连的Master节点，所以可以忽略；
     *    b. 调用isTimeToReportOffset方法判断是否需要向Master节点汇报同步偏移量，如果需要则调用reportSlaveMaxOffset方法将当前的消息同步偏移量发送给Master节点；
     *    c. 调用processReadEvent处理网络请求中的可读事件，也就是处理Master发送过来的消息，将消息存入CommitLog；
     */
    class HAClient extends ServiceThread {
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        private SocketChannel socketChannel;
        private Selector selector;
        private long lastWriteTimestamp = System.currentTimeMillis();

        /**
         * 当前的主从复制进度
         */
        private long currentReportedOffset = 0;
        /**
         * 已经处理的数据在读缓冲区中的位置,初始化为0
         */
        private int dispatchPosition = 0;
        /**
         * 读缓冲区，会将从socketChannel读入缓冲区
         */
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        /**
         * 发送主从同步消息拉取偏移量
         *
         * 在isTimeToReportOffset方法中，首先获取当前时间与上一次进行主从同步的时间间隔interval，如果时间间隔interval大于配置的发送心跳时间间隔
         * ，表示需要向Master节点发送从节点消息同步的偏移量，接下来会调用reportSlaveMaxOffset方法发送同步偏移量，也就是说从节点会定时向Master节点发送请求
         * ，反馈CommitLog中同步消息的偏移量.
         *
         * @return
         */
        private boolean isTimeToReportOffset() {
            // 获取距离上一次主从同步的间隔时间
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            // 判断是否超过了配置的发送心跳包时间间隔
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        /**
         * 发送同步偏移量,传入的参数是当前的主从复制偏移量currentReportedOffset
         *
         * @param maxOffset
         * @return
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            // 设置数据传输大小为8个字节
            this.reportOffset.limit(8);
            // 设置同步偏移量
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    // 向Master节点发送拉取偏移量
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            // 更新发送时间
            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * 处理网络可读事件
         *1. processReadEvent方法中处理了可读事件，也就是处理Master节点发送的同步数据， 首先从socketChannel中读取数据到byteBufferRead中
         * ，byteBufferRead是读缓冲区，读取数据的方法会返回读取到的字节数，对字节数大小进行判断：
         *  a. 如果可读字节数大于0表示有数据需要处理，调用dispatchReadRequest方法进行处理;
         *  b. 如果可读字节数为0表示没有可读数据，此时记录读取到空数据的次数，如果连续读到空数据的次数大于3次，将终止本次处理;
         *
         *
         * @return
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    // 从socketChannel中读取数据到byteBufferRead中，返回读取到的字节数
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        // 重置readSizeZeroTimes
                        readSizeZeroTimes = 0;
                        // 处理数据
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        // 记录读取到空数据的次数
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        /**
         *消息写入ComitLog
         *1. dispatchReadRequest方法中会将从节点读取到的数据写入CommitLog，dispatchPosition记录了已经处理的数据在读缓冲区中的位置
         * ，从读缓冲区byteBufferRead获取剩余可读取的字节数，如果可读数据的字节数大于一个消息头的字节数（12个字节），表示有数据还未处理完毕
         * ，反之表示消息已经处理完毕结束处理。对数据的处理逻辑如下：
         *      a. 从缓冲区中读取数据，首先获取到的是消息在master节点的物理偏移量masterPhyOffset；
         *      b. 向后读取8个字节，得到消息体内容的字节数bodySize；
         *      c. 获取从节点当前CommitLog的最大物理偏移量slavePhyOffset，如果不为0并且不等于masterPhyOffset，表示与Master节点的传输偏移量不一致
         *          ，也就是数据不一致，此时终止处理；
         *      d. 如果可读取的字节数大于一个消息头的字节数 + 消息体大小，表示有消息可处理，继续进行下一步；
         *      e. 计算消息体在读缓冲区中的起始位置，从读缓冲区中根据起始位置，读取消息内容，将消息追加到从节点的CommitLog中；
         *      f. 更新dispatchPosition的值为消息头大小 + 消息体大小，dispatchPosition之前的数据表示已经处理完毕；
         *
         *
         * @return
         */
        private boolean dispatchReadRequest() {
            // 消息头大小
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            int readSocketPos = this.byteBufferRead.position();

            // 开启循环不断读取数据
            while (true) {
                // 获取读取的字节数
                int diff = this.byteBufferRead.position() - this.dispatchPosition;
                // 如果字节数大于一个消息头的字节数
                if (diff >= msgHeaderSize) {
                    // 获取消息在master节点的物理偏移量
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    // 获取消息体大小
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    // 获取从节点当前CommitLog的最大物理偏移量
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        // 如果不一致结束处理
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    // 如果可读取的字节数大于一个消息头的字节数 + 消息体大小
                    if (diff >= (msgHeaderSize + bodySize)) {
                        // 将度缓冲区的数据转为字节数组
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPosition + msgHeaderSize);
                        this.byteBufferRead.get(bodyData);

                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        // 从读缓冲区中根据消息的位置，读取消息内容，将消息追加到从节点的CommitLog中
                        this.byteBufferRead.position(readSocketPos);
                        // 更新dispatchPosition的值为消息头大小+消息体大小
                        this.dispatchPosition += msgHeaderSize + bodySize;

                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        /**
         * 连接主节点
         *  1. connectMaster方法中会获取Master节点的地址，并转换为SocketAddress对象，然后向Master节点请求建立连接，并在selector注册OP_READ可读事件监听;
         *
         *
         *
         * @return
         * @throws ClosedChannelException
         */
        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                String addr = this.masterAddress.get();
                if (addr != null) {

                    // 将地址转为SocketAddress
                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        // 连接master
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            // 注册OP_READ可读事件监听
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                // 获取CommitLog中当前最大的偏移量
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                // 更新上次写入时间
                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 连接Master节点
                    if (this.connectMaster()) {

                        // 是否需要报告消息同步偏移量
                        if (this.isTimeToReportOffset()) {
                            // 向Master节点发送同步偏移量
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        // 等待事件产生
                        this.selector.select(1000);

                        // 处理读事件，也就是Master节点发送的数据
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
