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
package org.apache.rocketmq.example.quickstart.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 * 顺序消息
 */
public class ProducerOrder {
    public static void main(String[] args) throws MQClientException, InterruptedException, RemotingException, MQBrokerException {

        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer("my-group");
        producer.setNamesrvAddr("192.168.0.107:9876");
        producer.start();
        List<OrderStep> orderStepList = Stream.of(new OrderStep("orderA", 1001L, "创建")
                , new OrderStep("orderA", 1001L, "付款")
                , new OrderStep("orderA", 1001L, "完成")
                , new OrderStep("orderB", 1002L,"创建")
                , new OrderStep("orderB", 1002L,"付款")
                , new OrderStep("orderB", 1002L,"完成")
                , new OrderStep("orderC", 1003L)).collect(Collectors.toList());
        for (int i = 0; i < orderStepList.size(); i++) {
            Message message = new Message("orderTopic", "order", "i" + i, orderStepList.get(i).toString().getBytes());
            // arg1：消息对象 ； arg2：消息队列的选择器 ；arg3： 业务表示
            SendResult send = producer.send(message, new MessageQueueSelector() {
                /**
                 *
                 * @param mqs 队列集合
                 * @param msg 消息对象
                 * @param arg 业务标识的参数
                 * @return
                 */
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    long orderId = (long) arg;
                    long index = orderId % mqs.size();
                    return mqs.get((int) index);
                }
            }, orderStepList.get(i).getOrderId());
            System.out.println("发送结果：" + send);
        }
        /*
         * Shut down once the producer instance is not longer in use.
         */
        producer.shutdown();
    }

    static class OrderStep {
        private String des;
        private Long orderId;
        private String operation;

        public OrderStep(String des, Long orderId) {
            this.des = des;
            this.orderId = orderId;
        }

        public OrderStep(String des, Long orderId, String operation) {
            this.des = des;
            this.orderId = orderId;
            this.operation = operation;
        }

        public String getDes() {
            return des;
        }

        public void setDes(String des) {
            this.des = des;
        }

        public Long getOrderId() {
            return orderId;
        }

        public void setOrderId(Long orderId) {
            this.orderId = orderId;
        }

        @Override
        public String toString() {
            return "OrderStep{" +
                    "des='" + des + '\'' +
                    ", orderId=" + orderId +
                    '}';
        }
    }
}
