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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public class ConsumerGroupInfo {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final String groupName;
    /**
     * 记录了订阅的主题信息，key为topic，value为订阅信息
     */
    private final ConcurrentMap<String/* Topic */, SubscriptionData> subscriptionTable =
        new ConcurrentHashMap<String, SubscriptionData>();
    /**
     * key为消费者对应的channle，value为chanel信息
     */
    private final ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
        new ConcurrentHashMap<Channel, ClientChannelInfo>(16);
    private volatile ConsumeType consumeType;
    private volatile MessageModel messageModel;
    private volatile ConsumeFromWhere consumeFromWhere;
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel,
        ConsumeFromWhere consumeFromWhere) {
        this.groupName = groupName;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
    }

    public ClientChannelInfo findChannel(final String clientId) {
        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> next = it.next();
            if (next.getValue().getClientId().equals(clientId)) {
                return next.getValue();
            }
        }

        return null;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionTable() {
        return subscriptionTable;
    }

    public ConcurrentMap<Channel, ClientChannelInfo> getChannelInfoTable() {
        return channelInfoTable;
    }

    public List<Channel> getAllChannel() {
        List<Channel> result = new ArrayList<>();

        result.addAll(this.channelInfoTable.keySet());

        return result;
    }

    public List<String> getAllClientId() {
        List<String> result = new ArrayList<>();

        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> entry = it.next();
            ClientChannelInfo clientChannelInfo = entry.getValue();
            result.add(clientChannelInfo.getClientId());
        }

        return result;
    }

    public void unregisterChannel(final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo old = this.channelInfoTable.remove(clientChannelInfo.getChannel());
        if (old != null) {
            log.info("unregister a consumer[{}] from consumerGroupInfo {}", this.groupName, old.toString());
        }
    }

    public boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        final ClientChannelInfo info = this.channelInfoTable.remove(channel);
        if (info != null) {
            log.warn(
                "NETTY EVENT: remove not active channel[{}] from ConsumerGroupInfo groupChannelTable, consumer group: {}",
                info.toString(), groupName);
            return true;
        }

        return false;
    }

    /**
     * Channel变更
     *1. 在updateChannel方法中，首先将变更状态updated初始化为false，然后根据消费者的channel从channelInfoTable路由表中获取对应的ClientChannelInfo对象：
     *  1. 如果ClientChannelInfo对象获取为空，表示之前不存在该消费者的channel信息，将其加入到路由表中，变更状态置为true，表示消费者有变化；
     *  2. 如果获取不为空，判断clientid是否一致，如果不一致更新为最新的channel信息，但是变更状态updated不发生变化；
     *也就是说，如果注册的消费者之前不存在，那么将变更状态置为true，表示消费者数量发生了变化。
     *
     *
     * @param infoNew
     * @param consumeType
     * @param messageModel
     * @param consumeFromWhere
     * @return
     */
    public boolean updateChannel(final ClientChannelInfo infoNew, ConsumeType consumeType,
        MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        // 变更状态初始化为false
        boolean updated = false;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;

        // 从channelInfoTable中获取对应的Channel信息,
        ClientChannelInfo infoOld = this.channelInfoTable.get(infoNew.getChannel());
        // 如果为空
        if (null == infoOld) {
            ClientChannelInfo prev = this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            // 新增
            if (null == prev) {// 如果之前不存在
                log.info("new consumer connected, group: {} {} {} channel: {}", this.groupName, consumeType,
                    messageModel, infoNew.toString());
                // 变更状态置为true
                updated = true;
            }

            infoOld = infoNew;
        } else {
            // 如果之前存在，判断clientid是否一致，如果不一致更新为最新的channel
            if (!infoOld.getClientId().equals(infoNew.getClientId())) {
                log.error("[BUG] consumer channel exist in broker, but clientId not equal. GROUP: {} OLD: {} NEW: {} ",
                    this.groupName,
                    infoOld.toString(),
                    infoNew.toString());
                this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();
        infoOld.setLastUpdateTimestamp(this.lastUpdateTimestamp);

        return updated;
    }

    /**
     * 主题信息订阅变更:
     *1. updateSubscription方法中，主要判断了消费的主题订阅信息是否发生了变化，subscriptionTable中记录了之前记录的订阅信息：
     *  1. 判断是否有新增的主题订阅信息，主要是通过subscriptionTable是否存在某个主题进行判断的：
     *      1. 如果不存在，表示之前没有订阅过某个主题的信息，将其加入到subscriptionTable中，并将变更状态置为true，表示主题订阅信息有变化；
     *      2. 如果subscriptionTable中存在某个主题的订阅信息，表示之前就已订阅，将其更新为最新的，但是变更状态不发生变化；
     *  2. 判断是否有删除的主题，主要是通过subscriptionTable和subList的对比进行判断的，如果有删除的主题，将变更状态置为true；
     * 如果消费者订阅的主题发生了变化，比如有新增加的主题或者删除了某个主题的订阅，会被判断为主题订阅信息发生了变化。
     *
     * @param subList
     * @return
     */
    public boolean updateSubscription(final Set<SubscriptionData> subList) {
        boolean updated = false;

        // 遍历订阅的主题信息
        for (SubscriptionData sub : subList) {
            //根据主题获取订阅信息
            SubscriptionData old = this.subscriptionTable.get(sub.getTopic());
            // 如果获取为空
            if (old == null) {
                // 加入到subscriptionTable
                SubscriptionData prev = this.subscriptionTable.putIfAbsent(sub.getTopic(), sub);
                if (null == prev) {
                    // 变更状态置为true
                    updated = true;
                    log.info("subscription changed, add new topic, group: {} {}",
                        this.groupName,
                        sub.toString());
                }
            } else if (sub.getSubVersion() > old.getSubVersion()) {
                // 如果版本发生了变化
                if (this.consumeType == ConsumeType.CONSUME_PASSIVELY) {
                    log.info("subscription changed, group: {} OLD: {} NEW: {}",
                        this.groupName,
                        old.toString(),
                        sub.toString()
                    );
                }

                // 更新为最新的订阅信息
                this.subscriptionTable.put(sub.getTopic(), sub);
            }
        }

        Iterator<Entry<String, SubscriptionData>> it = this.subscriptionTable.entrySet().iterator();
        // 进行遍历，这一步主要是判断有没有取消订阅的主题
        while (it.hasNext()) {
            Entry<String, SubscriptionData> next = it.next();
            String oldTopic = next.getKey();

            boolean exist = false;
            // 遍历最新的订阅信息
            for (SubscriptionData sub : subList) {
                // 如果在旧的订阅信息中存在就终止，继续判断下一个主题
                if (sub.getTopic().equals(oldTopic)) {
                    exist = true;
                    break;
                }
            }

            // 走到这里，表示有取消订阅的主题
            if (!exist) {
                log.warn("subscription changed, group: {} remove topic {} {}",
                    this.groupName,
                    oldTopic,
                    next.getValue().toString()
                );

                // 进行删除
                it.remove();
                // 变更状态置为true
                updated = true;
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();

        return updated;
    }

    public Set<String> getSubscribeTopics() {
        return subscriptionTable.keySet();
    }

    public SubscriptionData findSubscriptionData(final String topic) {
        return this.subscriptionTable.get(topic);
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public String getGroupName() {
        return groupName;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }
}
