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
package org.apache.rocketmq.client.consumer.store;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Local storage implementation 对应广播模式<p/>
 * RocketMQ消费模式分为广播模式和集群模式，广播模式下消费进度保存在每个消费者端，集群模式下消费进度保存在Broker端。
 */
public class LocalFileOffsetStore implements OffsetStore {
    /**
     * 由于广播模式下消费进度保存在消费者端，所以需要从本地磁盘加载之前保存的消费进度文件。
     */
    public final static String LOCAL_OFFSET_STORE_DIR = System.getProperty(
        "rocketmq.client.localOffsetStoreDir",
        System.getProperty("user.home") + File.separator + ".rocketmq_offsets");
    private final static InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String groupName;
    private final String storePath;
    /**
     * 存储消息队列对应的拉取偏移量:
     *  KEY为消息队列，value为该消息队列对应的拉取偏移量。
     */
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable =
        new ConcurrentHashMap<MessageQueue, AtomicLong>();

    public LocalFileOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
        // 在LocalFileOffsetStore的构造函数中可以看到，对拉取偏移量的保存文件路径进行了设置
        // ，为LOCAL_OFFSET_STORE_DIR + 客户端ID + 消费组名称 + offsets.json，从名字上看，消费进度的数据格式是以JSON的形式进行保存的
        this.storePath = LOCAL_OFFSET_STORE_DIR + File.separator +
            this.mQClientFactory.getClientId() + File.separator +
            this.groupName + File.separator +
            "offsets.json";
    }

    @Override
    public void load() throws MQClientException {
        // 从本地读取拉取偏移量
        OffsetSerializeWrapper offsetSerializeWrapper = this.readLocalOffset();
        if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
            // 加入到offsetTable中
            offsetTable.putAll(offsetSerializeWrapper.getOffsetTable());

            for (MessageQueue mq : offsetSerializeWrapper.getOffsetTable().keySet()) {
                AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                log.info("load consumer's offset, {} {} {}",
                    this.groupName,
                    mq,
                    offset.get());
            }
        }
    }

    /**
     * 在更新拉取进度的时候，从offsetTable中获取当前消息队列的拉取偏移量，如果为空，则新建并保存到offsetTable中，否则获取之前已经保存
     * 的偏移量，对值进行更新，需要注意这里只是更新了offsetTable中的数据，并没有持久化到磁盘，持久化的操作在persistAll方法中.
     * updateOffset更新只是将内存中的数据进行了更改，并未保存到磁盘中，持久化的操作是在persistAll方法中实现的.
     *
     *
     *
     *
     * @param mq
     * @param offset
     * @param increaseOnly
     */
    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            // 获取之前的拉取进度
            AtomicLong offsetOld = this.offsetTable.get(mq);
            if (null == offsetOld) {
                // 如果之前不存在，进行创建
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            // 如果不为空
            if (null != offsetOld) {
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    // 更新拉取偏移量
                    offsetOld.set(offset);
                }
            }
        }
    }

    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return -1;
                    }
                }
                case READ_FROM_STORE: {
                    OffsetSerializeWrapper offsetSerializeWrapper;
                    try {
                        offsetSerializeWrapper = this.readLocalOffset();
                    } catch (MQClientException e) {
                        return -1;
                    }
                    if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
                        AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                        if (offset != null) {
                            this.updateOffset(mq, offset.get(), false);
                            return offset.get();
                        }
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }

    /**
     * 1.updateOffset更新只是将内存中的数据进行了更改，并未保存到磁盘中，持久化的操作是在persistAll方法中实现的：
     *  a. 创建OffsetSerializeWrapper对象
     *  b. 遍历LocalFileOffsetStore的offsetTable，将数据加入到OffsetSerializeWrapper的OffsetTable中
     *  c. 将OffsetSerializeWrapper转为JSON
     *  d. 调用string2File方法将JSON数据保存到磁盘文件
     *
     * @param mqs
     */
    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty())
            return;

        // 创建
        OffsetSerializeWrapper offsetSerializeWrapper = new OffsetSerializeWrapper();
        // 遍历offsetTable
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            if (mqs.contains(entry.getKey())) {
                // 获取拉取偏移量
                AtomicLong offset = entry.getValue();
                // 加入到OffsetSerializeWrapper的OffsetTable中
                offsetSerializeWrapper.getOffsetTable().put(entry.getKey(), offset);
            }
        }

        // 将对象转为JSON
        String jsonString = offsetSerializeWrapper.toJson(true);
        if (jsonString != null) {
            try {
                // 将JSON数据保存到磁盘文件
                MixAll.string2File(jsonString, this.storePath);
            } catch (IOException e) {
                log.error("persistAll consumer offset Exception, " + this.storePath, e);
            }
        }
    }

    @Override
    public void persist(MessageQueue mq) {
    }

    @Override
    public void removeOffset(MessageQueue mq) {

    }

    @Override
    public void updateConsumeOffsetToBroker(final MessageQueue mq, final long offset, final boolean isOneway)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());

        }
        return cloneOffsetTable;
    }

    /**
     * 从本地加载文件
     *
     *
     * @return
     * @throws MQClientException
     */
    private OffsetSerializeWrapper readLocalOffset() throws MQClientException {
        String content = null;
        try {
            // 读取文件
            content = MixAll.file2String(this.storePath);
        } catch (IOException e) {
            log.warn("Load local offset store file exception", e);
        }
        if (null == content || content.length() == 0) {
            return this.readLocalOffsetBak();
        } else {
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                // 序列化
                offsetSerializeWrapper =
                    OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception, and try to correct", e);
                return this.readLocalOffsetBak();
            }

            return offsetSerializeWrapper;
        }
    }

    private OffsetSerializeWrapper readLocalOffsetBak() throws MQClientException {
        String content = null;
        try {
            content = MixAll.file2String(this.storePath + ".bak");
        } catch (IOException e) {
            log.warn("Load local offset store bak file exception", e);
        }
        if (content != null && content.length() > 0) {
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                offsetSerializeWrapper =
                    OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception", e);
                throw new MQClientException("readLocalOffset Exception, maybe fastjson version too low"
                    + FAQUrl.suggestTodo(FAQUrl.LOAD_JSON_EXCEPTION),
                    e);
            }
            return offsetSerializeWrapper;
        }

        return null;
    }
}
