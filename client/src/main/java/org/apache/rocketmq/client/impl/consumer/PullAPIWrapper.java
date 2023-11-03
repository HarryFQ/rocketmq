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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class PullAPIWrapper {
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String consumerGroup;
    private final boolean unitMode;

    /**
     * KEY为消息队列，VALUE为建议的Broker ID
     * 通过调用关系可知，在updatePullFromWhichNode方法中更新了pullFromWhichNodeTable的值，而updatePullFromWhichNode方法又是被processPullResult方法调用的
     * ，消费者向Broker发送拉取消息请求后，Broker对拉取请求进行处理时会设置一个broker ID（后面会讲到），建议下次从这个Broker拉取消息
     * ，消费者对拉取请求返回的响应数据进行处理时会调用processPullResult方法，在这里将建议的BrokerID取出，调用updatePullFromWhichNode方法将其加入到了pullFromWhichNodeTable中：
     *
     *
     */
    private ConcurrentMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable =
        new ConcurrentHashMap<MessageQueue, AtomicLong>(32);
    private volatile boolean connectBrokerByUser = false;
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    private Random random = new Random(System.currentTimeMillis());
    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult,
        final SubscriptionData subscriptionData) {
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        // 将拉取消息请求返回的建议Broker ID，加入到pullFromWhichNodeTable中
        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());
        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

            List<MessageExt> msgListFilterAgain = msgList;
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            /// 过滤消息
            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            for (MessageExt msg : msgListFilterAgain) {
                String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(traFlag)) {
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                }
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,
                    Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,
                    Long.toString(pullResult.getMaxOffset()));
                msg.setBrokerName(mq.getBrokerName());
            }

            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        pullResultExt.setMessageBinary(null);

        return pullResult;
    }

    /**
     * 1. 在updatePullFromWhichNode方法中更新了pullFromWhichNodeTable的值，而updatePullFromWhichNode方法又是被processPullResult方法调用的
     * ，消费者向Broker发送拉取消息请求后，Broker对拉取请求进行处理时会设置一个broker ID（后面会讲到），建议下次从这个Broker拉取消息
     * ，消费者对拉取请求返回的响应数据进行处理时会调用processPullResult方法，在这里将建议的BrokerID取出，调用updatePullFromWhichNode方法将其加入到了pullFromWhichNodeTable中.
     *
     * 2. 接下来去看下是根据什么条件决定选择哪个Broker的?
     *  Broker在处理消费者拉取请求时，会调用PullMessageProcessor的processRequest方法，首先会调用MessageStore的getMessage方法获取消息内容
     *  ，在返回的结果GetMessageResult中设置了一个是否建议从Slave节点拉取的属性(这个值的设置稍后再说)，会根据是否建议从slave节点进行以下处理：
     *      a. 如果建议从slave节点拉取消息，会调用subscriptionGroupConfig订阅分组配置的getWhichBrokerWhenConsumeSlowly方法获取从节点将ID设置到响应中
     *          ，否则下次依旧建议从主节点拉取消息，将MASTER节点的ID设置到响应中；
     *      b. 判断当前Broker的角色，如果是slave节点，并且配置了不允许从slave节点读取数据（SlaveReadEnable = false），此时依旧建议从主节点拉取消息
     *          ，将MASTER节点的ID设置到响应中；
     *      c. 如果开启了允许从slave节点读取数据（SlaveReadEnable = true），有以下两种情况：
     *          1. 如果建议从slave节点拉消息，从订阅分组配置中获取从节点的ID，将ID设置到响应中；
     *          2. 如果不建议从slave节点拉取消息，从订阅分组配置中获取设置的Broker Id；当然，如果未开启允许从Slave节点读取数据，下次依旧建议从Master节点拉取；
     *
     *总结
     * 1. 消费者在启动后需要向Broker发送拉取消息的请求，Broker收到请求后会根据消息的拉取进度，返回一个建议的BrokerID，并设置到响应中返回
     *  ，消费者处理响应时将建议的BrokerID放入pullFromWhichNodeTable，下次拉去消息的时候从pullFromWhichNodeTable中取出，并向其发送请求拉取消息.
     * @param mq
     * @param brokerId
     */
    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            // 向pullFromWhichNodeTable中添加数据
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    /**
     * PullAPIWrapper中主要是获取了Broker的地址，然后创建拉取请求头PullMessageRequestHeader，设置拉取的相关信息，然后调用MQClientAPIImpl的pullMessage拉取消息：
     *
     *
     *
     * @param mq
     * @param subExpression
     * @param expressionType
     * @param subVersion
     * @param offset
     * @param maxNums
     * @param sysFlag
     * @param commitOffset
     * @param brokerSuspendMaxTimeMillis
     * @param timeoutMillis
     * @param communicationMode
     * @param pullCallback
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    public PullResult pullKernelImpl(
        final MessageQueue mq,
        final String subExpression,
        final String expressionType,
        final long subVersion,
        final long offset,
        final int maxNums,
        final int sysFlag,
        final long commitOffset,
        final long brokerSuspendMaxTimeMillis,
        final long timeoutMillis,
        final CommunicationMode communicationMode,
        final PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        // 根据BrokerName获取Broker信息
        // 调用recalculatePullFromWhichNode方法获取Broker ID，再调用findBrokerAddressInSubscribe根据ID获取Broker的相关信息
        FindBrokerResult findBrokerResult =
            this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                this.recalculatePullFromWhichNode(mq), false);
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult =
                this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                    this.recalculatePullFromWhichNode(mq), false);
        }

        if (findBrokerResult != null) {
            {
                // check version
                if (!ExpressionType.isTagType(expressionType)
                    && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                    throw new MQClientException("The broker[" + mq.getBrokerName() + ", "
                        + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                }
            }
            int sysFlagInner = sysFlag;

            if (findBrokerResult.isSlave()) {
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }
            // 创建拉取消息的请求头
            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            // 设置消费组
            requestHeader.setConsumerGroup(this.consumerGroup);
            // 设置主题
            requestHeader.setTopic(mq.getTopic());
            // 设置队列ID
            requestHeader.setQueueId(mq.getQueueId());
            // 设置拉取偏移量
            requestHeader.setQueueOffset(offset);
            // 设置拉取最大消息个数
            requestHeader.setMaxMsgNums(maxNums);
            // 设置系统标识
            requestHeader.setSysFlag(sysFlagInner);
            // 设置commit偏移量
            requestHeader.setCommitOffset(commitOffset);
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            // 设置订阅主题表达式
            requestHeader.setSubscription(subExpression);
            requestHeader.setSubVersion(subVersion);
            requestHeader.setExpressionType(expressionType);

            // 获取Broker地址
            String brokerAddr = findBrokerResult.getBrokerAddr();
            if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                brokerAddr = computePullFromWhichFilterServer(mq.getTopic(), brokerAddr);
            }

            PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(
                brokerAddr,
                requestHeader,
                timeoutMillis,
                communicationMode,
                pullCallback);

            return pullResult;
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    /**
     * 在recalculatePullFromWhichNode方法中，会从pullFromWhichNodeTable中根据消息队列获取一个建议的Broker ID，如果获取为空就返回Master节点的Broker ID
     * ，ROCKETMQ中Master角色的Broker ID为0，既然从pullFromWhichNodeTable中可以知道从哪个Broker拉取数据，那么pullFromWhichNodeTable中的数据又是从哪里来的？
     *
     * @param mq
     * @return
     */
    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        // 从pullFromWhichNodeTable中获取建议的broker ID
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        // 返回Master Broker ID
        return MixAll.MASTER_ID;
    }

    private String computePullFromWhichFilterServer(final String topic, final String brokerAddr)
        throws MQClientException {
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

            if (list != null && !list.isEmpty()) {
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: "
            + topic, null);
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }
}
