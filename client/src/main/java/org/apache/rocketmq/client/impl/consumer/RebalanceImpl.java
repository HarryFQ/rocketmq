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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public abstract class RebalanceImpl {
    protected static final InternalLogger log = ClientLogger.getLog();
    /**
     * 处理队列表，KEY为消息队列，VALUE为对应的处理信息
     * key: 消息队列；
     * value： 处理队列
     */
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
        new ConcurrentHashMap<String, Set<MessageQueue>>();
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
        new ConcurrentHashMap<String, SubscriptionData>();
    protected String consumerGroup;
    protected MessageModel messageModel;
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    protected MQClientInstance mQClientFactory;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    this.consumerGroup,
                    this.mQClientFactory.getClientId(),
                    mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            Set<MessageQueue> mqs = result.get(mq.getBrokerName());
            if (null == mqs) {
                mqs = new HashSet<MessageQueue>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    /**
     * 1. 如果加锁成功构建拉取请求进行消息拉取，如果加锁失败，则跳过继续处理下一个消息队列
     *
     * @param mq
     * @return
     */
    public boolean lock(final MessageQueue mq) {
        // 获取broker信息
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            // 构建加锁请求
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            // 设置要加锁的消息队列
            requestBody.getMqSet().add(mq);

            try {
                // 发送加锁请求
                Set<MessageQueue> lockedMq =
                    this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    // 如果加锁成功设置成功标记
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                boolean lockOK = lockedMq.contains(mq);
                log.info("the message queue lock {}, {} {}",
                    lockOK ? "OK" : "Failed",
                    this.consumerGroup,
                    mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    /**
     * 消息队列加锁
     *  1. 在RebalanceImpl的lockAll方法中，首先从处理队列表中获取当前消费者订阅的所有消息队列MessageQueue信息，返回数据是一个MAP，key为broker名称，value为broker下的消息队列，接着对MAP进行遍历，处理每一个broker下的消息队列：
     *      a. 获取broker名称，根据broker名称查找broker的相关信息；
     *      b. 构建加锁请求，在请求中设置要加锁的消息队列，然后将请求发送给broker，表示要对这些消息队列进行加锁；
     *      c. 加锁请求返回的响应结果中包含了加锁成功的消息队列，此时遍历加锁成功的消息队列，将消息队列对应的ProcessQueue中的locked属性置为true表示该消息队列已加锁成功；
     *      d. 处理加锁失败的消息队列，如果响应中未包含某个消息队列的信息，表示此消息队列加锁失败，需要将其对应的ProcessQueue对象中的locked属性置为false表示加锁失败；
     *
     *  2. 在【RocketMQ】消息的拉取一文中讲到，消费者需要先向Broker发送拉取消息请求，从Broker中拉取消息，拉取消息请求构建在RebalanceImpl
     *     的updateProcessQueueTableInRebalance方法中，拉取消息的响应结果处理在PullCallback的onSuccess方法中，接下来看下顺序消费时在这两个过程中是如何处理的.
     *
     */
    public void lockAll() {
        // 从处理队列表中获取broker对应的消息队列，key为broker名称，value为broker下的消息队列
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        // 遍历订阅的消息队列
        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            // broker名称
            final String brokerName = entry.getKey();
            // 获取消息队列
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            // 根据broker名称获取broker信息
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                // 构建加锁请求
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                // 设置消费者组
                requestBody.setConsumerGroup(this.consumerGroup);
                // 设置ID
                requestBody.setClientId(this.mQClientFactory.getClientId());
                // 设置要加锁的消息队列
                requestBody.setMqSet(mqs);

                try {
                    // 批量进行加锁，返回加锁成功的消息队列
                    Set<MessageQueue> lockOKMQSet =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                    // 遍历加锁成功的队列
                    for (MessageQueue mq : lockOKMQSet) {
                        // 从处理队列表中获取对应的处理队列对象
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        // 如果不为空，设置locked为true表示加锁成功
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }

                            // 设置加锁成功标记
                            processQueue.setLocked(true);
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }
                    // 处理加锁失败的消息队列
                    for (MessageQueue mq : mqs) {
                        if (!lockOKMQSet.contains(mq)) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                // 设置加锁失败标记
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    /**
     * RebalanceImpl的doRebalance处理逻辑如下：
     *  1. 获取订阅的主题信息集合，在订阅处理章节中，可以看到将订阅的主题信息封装成了SubscriptionData并加入到了RebalanceImpl中
     *  2. 对获取到的订阅主题信息集合进行遍历，调用rebalanceByTopic对每一个主题进行负载均衡
     * @param isOrder
     */
    public void doRebalance(final boolean isOrder) {
        // 获取订阅的主题信息
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            // 遍历所有订阅的主题
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    // 根据主题进行负载均衡
                    this.rebalanceByTopic(topic, isOrder);
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }
            }
        }

        this.truncateMessageQueueNotMyTopic();
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    /**
     * 负载均衡实际处理方法,根据主题进行负载均衡
     * 1. rebalanceByTopic方法中根据消费模式进行了判断然后对主题进行负载均衡，这里我们关注集群模式下的负载均衡：
     *  a. 从topicSubscribeInfoTable中根据主题获取对应的消息队列集合
     *  b. 根据主题信息和消费者组名称，获取所有订阅了该主题的消费者ID集合
     *  c. 如果主题对应的消息队列集合和消费者ID都不为空，对消息队列集合和消费ID集合进行排序
     *  d. 获取分配策略，根据分配策略，为当前的消费者分配对应的消费队列，RocketMQ默认提供了以下几种分配策略：
     *      1. AllocateMessageQueueAveragely：平均分配策略，根据消息队列的数量和消费者的个数计算每个消费者分配的队列个数。
     *      2. AllocateMessageQueueAveragelyByCircle：平均轮询分配策略，将消息队列逐个分发给每个消费者。
     *      3. AllocateMessageQueueConsistentHash：根据一致性 hash进行分配。
     *      4. llocateMessageQueueByConfig：根据配置，为每一个消费者配置固定的消息队列 。
     *      5. AllocateMessageQueueByMachineRoom：分配指定机房下的消息队列给消费者。
     *      6. AllocateMachineRoomNearby：优先分配给同机房的消费者。
     *  e. 根据最新分配的消息队列，调用updateProcessQueueTableInRebalance更新当前消费者消费的队列信息
     *
     * @param topic
     * @param isOrder
     */
    private void rebalanceByTopic(final String topic, final boolean isOrder) {
        switch (messageModel) {
            case BROADCASTING: {
                // 广播模式
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}",
                            consumerGroup,
                            topic,
                            mqSet,
                            mqSet);
                    }
                } else {
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            case
                    // 集群模式
                    CLUSTERING: {
                // 获取topic 下的所有订阅的消息队列(根据主题获取所有订阅的消息队列)
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                // 获取topic 和consumerGroup下的所有的consumerId , （根据topic 和consumerGroup 获取所有订阅了该主题的消费者id）
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                // 如果都不为空
                if (mqSet != null && cidAll != null) {
                    List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                    mqAll.addAll(mqSet);

                    // 对消息队列排序
                    Collections.sort(mqAll);
                    // 对消费者排序
                    Collections.sort(cidAll);
                    // 获取分配策略
                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    List<MessageQueue> allocateResult = null;
                    try {
                        // 根据分配策略，为当前的消费者分配消费队列
                        allocateResult = strategy.allocate(
                            this.consumerGroup,
                            this.mQClientFactory.getClientId(),
                            mqAll,
                            cidAll);
                    } catch (Throwable e) {
                        log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(),
                            e);
                        return;
                    }
                    // 分配给当前消费的消费队列
                    Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                    if (allocateResult != null) {
                        // 将分配结果加入到结果集合中
                        allocateResultSet.addAll(allocateResult);
                    }

                    // 根据分配信息更新处理队列
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    if (changed) {
                        log.info(
                            "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                            strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                            allocateResultSet.size(), allocateResultSet);
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }

    /**
     * 负载均衡 更新处理队列表
     * 1. RebalanceImpl中使用了一个ConcurrentMap类型的处理队列表存储消息队列及对应的队列处理信息，updateProcessQueueTableInRebalance
     * 方法的入参中topic表示当前要进行负载均衡的主题，mqSet中记录了重新分配给当前消费者的消息队列，主要处理逻辑如下：
     *  a. 获取处理队列表processQueueTable进行遍历，处理每一个消息队列，如果队列表为空直接进入第2步：
     *      1. 判断消息队列所属的主题是否与方法中指定的主题一致，如果不一致继续遍历下一个消息队列;
     *         如果主题一致，判断mqSet中是否包含当前正在遍历的队列，如果不包含，说明此队列已经不再分配给当前的消费者进行消费，需要将消息队列置为dropped，表示删除
     *  b. 创建消息拉取请求集合pullRequestList，并遍历本次分配的消息队列集合，如果某个消息队列不在processQueueTable中，需要进行如下处理：
     *      1. 计算消息拉取偏移量，如果消息拉取偏移量大于0，创建ProcessQueue，并放入处理队列表中processQueueTable
     *      2. 构建PullRequest，设置消息的拉取信息，并加入到拉取消息请求集合pullRequestList中
     *  c. 调用dispatchPullRequest处理拉取请求集合中的数据
     * 可以看到，经过这一步，如果分配给当前消费者的消费队列不在processQueueTable中，就会构建拉取请求PullRequest，然后调用dispatchPullRequest处理消息拉取请求。
     *
     * 2. 在使用顺序消息时，会周期性的对订阅的消息队列进行加锁，不过由于负载均衡等原因，有可能给当前消费者分配新的消息队列，此时可能还未来得及通过定时任务加锁
     *    ，所以消费者在构建消息拉取请求前会再次进行判断，如果processQueueTable中之前未包含某个消息队列，会先调用lock方法进行加锁，lock方法的实现逻辑与lockAll基本一致
     *    ，如果加锁成功构建拉取请求进行消息拉取，如果加锁失败，则跳过继续处理下一个消息队列.
     *
     *
     *
     * @param topic 表示当前要进行负载均衡的主题
     * @param mqSet 中记录了重新分配给当前消费者的消息队列
     * @param isOrder
     * @return
     */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
        final boolean isOrder) {
        boolean changed = false;

        // 处理队列表
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            // 获取消息队列
            MessageQueue mq = next.getKey();
            // 获取处理队列
            ProcessQueue pq = next.getValue();

            // 主题是否一致
            if (mq.getTopic().equals(topic)) {

                // 如果重新分配的消息队列集合中不包含当前的消息队列
                if (!mqSet.contains(mq)) {
                    // 删除标记置为true ，设置为dropped
                    pq.setDropped(true);
                    // 持久化偏移量，移除offset
                    if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                        it.remove();
                        changed = true;
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }
                } else if (pq.isPullExpired()) {// 在processQueueTable中找出在分配到的队列集合 中的消息队列 ，并判断processQueue 是否过期
                        // 是否过期
                    switch (this.consumeType()) {
                        case CONSUME_ACTIVELY: // 拉模式
                            break;
                        case CONSUME_PASSIVELY:// 推的模式
                            // 设置为删除
                            pq.setDropped(true);
                            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                                it.remove();
                                changed = true;
                                log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                    consumerGroup, mq);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        // 创建拉取请求集合
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
        // 遍历本次分配的消息队列集合
        for (MessageQueue mq : mqSet) {
            // 前面将不在processQueueTable 中不在MQSet中的MessageQueue移除了，并且将过期的processQueue也移除了。
            // 如果之前不在processQueueTable中
            if (!this.processQueueTable.containsKey(mq)) {
                // 如果是顺序消费，调用lock方法进行加锁，如果加锁失败不往下执行，继续处理下一个消息队列
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    continue;
                }

                // 移除 当前messageQueue的offset
                this.removeDirtyOffset(mq);
                // 创建ProcessQueue
                ProcessQueue pq = new ProcessQueue();
                // 计算消息拉取偏移量
                long nextOffset = this.computePullFromWhere(mq);
                // 如果偏移量大于等于0
                if (nextOffset >= 0) {
                    // 放入处理队列表中
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    // 如果之前已经存在，不需要进行处理
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        // 如果之前不存在，构建PullRequest，之后会加入到阻塞队列中，拉取消息
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        PullRequest pullRequest = new PullRequest();
                        // 设置消费组
                        pullRequest.setConsumerGroup(consumerGroup);
                        // 设置拉取偏移量
                        pullRequest.setNextOffset(nextOffset);
                        // 设置消息队列
                        pullRequest.setMessageQueue(mq);
                        // 设置处理队列
                        pullRequest.setProcessQueue(pq);
                        // 加入到拉取消息请求集合
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }
        }

        // todo DEBUG 看一下
        // 添加消息拉取请求，{@link org.apache.rocketmq.client.impl.consumer.PullMessageService.run} 这里回死循环执行
        this.dispatchPullRequest(pullRequestList);

        return changed;
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
        final Set<MessageQueue> mqDivided);

    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
    }
}
