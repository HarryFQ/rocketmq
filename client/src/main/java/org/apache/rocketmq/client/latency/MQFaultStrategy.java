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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 主要是通过调用TopicPublishInfo中的相关方法进行消息队列选择的
     * 开启故障延迟机制：
     *  1. 如果启用了故障延迟机制，会遍历TopicPublishInfo中存储的消息队列列表，
     *      对计数器增1，轮询选择一个消息队列，接着会判断消息队列所属的Broker是否可用，如果Broker可用返回消息队列即可.
     *      如果选出的队列所属Broker不可用，会调用latencyFaultTolerance的pickOneAtLeast方法（下面会讲到）选择一个Broker，从tpInfo中获取此Broker可写的队列数量，如果数量大于0，调用selectOneMessageQueue()方法选择一个队列。
     *      如果故障延迟机制未选出消息队列，依旧会调用selectOneMessageQueue()选择出一个消息队列。
     * 未开启故障延迟机制：
     *  1.直接调用的selectOneMessageQueue(String lastBrokerName)方法并传入上一次使用的Broker名称进行选择。
     *
     * @param tpInfo topic 发布的信息
     * @param lastBrokerName
     * @return
     * 如果某个主题所在的所有Broker都处于不可用状态，此时调用pickOneAtLeast方法尽量选择延迟时间最短、规避时间最短（排序后的失败条目中靠前的元素）的Broker作为此次发生消息的Broker。
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // 开启了故障延迟机制
        if (this.sendLatencyFaultEnable) {
            try {
                // 获取当前线程 存储的索引 (已经加1了)
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                // 遍历TopicPublishInfo中存储的消息队列列表
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    // 在加一取模 ， 轮询选择一个消息队列
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    // 如果下标小于0，则使用0
                    if (pos < 0)
                        pos = 0;
                    // 根据下标获取消息队列
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 判断消息队列所属的Broker是否可用,如果可用返回当前选择的消息队列，退避时间是否已达到
                    // isAvailable方法来判断Broker是否可用，而LatencyFaultToleranceImpl的isAvailable方法又是调用Broker对应 FaultItem的isAvailable方法来判断的。
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }
                // 如果未获取到可用的Broker
                // 调用pickOneAtLeast选择一个
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                // 从tpInfo中获取Broker可写的队列数量
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                // 如果可写的队列数量大于0
                if (writeQueueNums > 0) {
                    // 选择一个消息队列
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        // 设置消息队列所属的Broker
                        mq.setBrokerName(notBestBroker);
                        // 设置QueueId
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    // 返回消息队列
                    return mq;
                } else {
                    // 移除broker
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            // 如果故障延迟机制未选出消息队列，调用selectOneMessageQueue选择消息队列
            return tpInfo.selectOneMessageQueue();
        }
        // 根据上一次使用的BrokerName获取消息队列
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 更新失败条目
     * @param brokerName Broker名称
     * @param currentLatency 发送消息耗时：请求结束时间 - 开始时间
     * @param isolation
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            // 计算duration，isolation为true时使用30000，否则使用发送消息的耗时时间currentLatency
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            // 更新到latencyFaultTolerance中
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            // 两个数组取下标
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
