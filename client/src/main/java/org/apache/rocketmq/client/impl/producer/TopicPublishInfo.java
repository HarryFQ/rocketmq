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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

public class TopicPublishInfo {
    private boolean orderTopic = false;
    private boolean haveTopicRouterInfo = false;
    /**
     * 记录了主题名称、主题所属的Broker名称和队列ID
     */
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
    /**
     * 计数器，选择消息队列的时候增1，以此达到轮询的目的
     */
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    /**
     * 从NameServer查询到的主题对应的路由数据，包含了队列和Broker的相关数据（brokerData）
     */
    private TopicRouteData topicRouteData;

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    /**
     * selectOneMessageQueue方法中，如果上一次选择的BrokerName为空，则调用无参的selectOneMessageQueue方法选择消息队列，也是默认的选择方式
     * ，首先对计数器增一，然后用计数器的值对messageQueueList列表的长度取余得到下标值pos，再从messageQueueList中获取pos位置的元素，以此达到轮询从messageQueueList列表中选择消息队列的目的。
     *
     * 如果传入的BrokerName不为空，遍历messageQueueList列表，同样对计数器增一，并对messageQueueList列表的长度取余，选取一个消息队列
     * ，不同的地方是选择消息队列之后，会判断消息队列所属的Broker是否与上一次选择的Broker名称一致，如果一致则继续循环，轮询选择下一个消息队列
     * ，也就是说，如果上一次选择了某个Broker发送消息，本次将不会再选择这个Broker，当然如果最后仍未找到满足要求的消息队列，则仍旧使用默认的选择方式
     * ，也就是调用无参的selectOneMessageQueue方法进行选择。
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName == null) {
            // 如果上一次选择的BrokerName为空
            // 调用无参的选择消息队列的方法
            return selectOneMessageQueue();
        } else {
            // 遍历消息队列列表
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                // 计数器增1
                int index = this.sendWhichQueue.getAndIncrement();
                // 对长度取余
                int pos = Math.abs(index) % this.messageQueueList.size();
                if (pos < 0)
                    pos = 0;
                // 获取消息队列，也就是使用使用轮询的方式选择消息队列
                MessageQueue mq = this.messageQueueList.get(pos);
                // 如果队列所属的Broker与上一次选择的不同，返回消息队列
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            // 使用默认方式选择
            return selectOneMessageQueue();
        }
    }

    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.getAndIncrement();
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0)
            pos = 0;
        return this.messageQueueList.get(pos);
    }

    public int getQueueIdByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}
