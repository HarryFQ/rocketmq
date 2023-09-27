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
package org.apache.rocketmq.example.quickstart.transcation;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class ProducerTranscation {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        // 事务消息生产者
        TransactionMQProducer producer = new TransactionMQProducer("my-Transcation");
        // 链接nameserver
        producer.setNamesrvAddr("192.168.0.107:9876");
        // 设置事务消息监听器
        producer.setTransactionListener(new TransactionListener() {
            /**
             *在该方法中执行本地事务
             * @param msg Half(prepare) message
             * @param arg Custom business parameter
             * @return
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                System.out.println("executeLocalTransaction:msg:" + msg.getTags());
                // 如果是tag1 就提交消息
                if (StringUtils.equals("tag1", msg.getTags())) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if (StringUtils.equals("tag2", msg.getTags())) {
                    // 如果是tag2 就回滚消息
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
                // 如果是tag 就回查
                return LocalTransactionState.UNKNOW;
            }
            /**
             * MQ 进行消息事务回查本地消息
             * @param msg Check message
             * @return
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                System.out.println("消息回查的tag:"+msg.getTags());

                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();
        String[] tags = {"tag1", "tag2", "tag3"};
        for (int i = 0; i < 3; i++) {
            try {
                Message msg = new Message("TopicTranscation" /* Topic */,
                        tags[i % 3] /* Tag */,//不同tag 区分不同类的消息
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );
                // 可以将消息应用到整个producer,或者某一条消息
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(2000);
            }
        }
        // producer.shutdown();
    }
}
