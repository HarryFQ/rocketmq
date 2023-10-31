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
package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 事物消息的监听器
 * 1. 提供了本地事务执行和状态回查的接口，executeLocalTransaction方法用于执行我们的本地事务，checkLocalTransaction是一种补偿机制
 * ，在异常情况下如果未收到事务的提交请求，会调用此方法进行事务状态查询，以此决定是否将事务进行提交/回滚
 *
 * 2. 它使用两阶段提交协议实现事务消息，同时增加补偿机制定时对事务的状态进行回查，来处理未提交/回滚的事务。
 *
 * 3. 发送事务消息分为两个阶段：
 *
 *  第一阶段：生产者向Broker发送half（prepare）消息，生产者发送事务消息的时候，消息不会直接存入对应的主题中，而是先将消息存入RMQ_SYS_TRANS_HALF_TOPIC主题中，此时消息对消费者不可见，不能被消费者消费，称为half消息，half消息发送成功之后，开始执行本地事务。
 *
 *  第二阶段：提交阶段，根据第一阶段的本地事务执行结果来决定是提交事务还是回滚事务，提交或者回滚的事务会从RMQ_SYS_TRANS_HALF_TOPIC中删除，对于提交的事务消息，会将消息投送到实际的主题队列中，之后消费者可以从队列中拉取到消息进行消费，对于回滚的事务消息，直接从RMQ_SYS_TRANS_HALF_TOPIC主题中删除即可。
 *
 * 注意：由于RocketMQ追加写的性能并不会直接从RMQ_SYS_TRANS_HALF_TOPIC队列中删除消息，而是使用了另外一个队列，将已提交或者回滚的事务放入到OP队列中，在补偿机制对half消息进行检查的时候会从OP中判断是消息是否已经提交或者回滚。
 *
 * 4. 补偿机制
 *  两阶段提交事务的过程中，任一阶段出现异常都有可能导致事务未能成功的进行提交/回滚，所以需要增加一种补偿机制，定时对RMQ_SYS_TRANS_HALF_TOPIC主题中的half消息进行处理。
 * RocketMQ使用了一种回查机制，在处理half消息时，对该消息的本地事务执行状态进行回查，根据回查结果决定是否需要提交/回滚，或者是等待下一次回查。
 *
 * 发送消息入口： {@link TransactionMQProducer#sendMessageInTransaction(Message, Object)}
 */
public interface TransactionListener {
    /**
     * When send transactional prepare(half) message succeed, this method will be invoked to execute local transaction.
     * 用于执行我们的本地事务 并返回事物结果，当投递到half 队列中后回执行此方法。
     * @param msg Half(prepare) message
     * @param arg Custom business parameter
     * @return Transaction state
     */
    LocalTransactionState executeLocalTransaction(final Message msg, final Object arg);

    /**
     * When no response to prepare(half) message. broker will send check message to check the transaction status, and this
     * method will be invoked to get local transaction status.
     * 事物回查方法 ，在本地事物没有提交的时候，broker 回查当前事物的状态。
     * checkLocalTransaction是一种补偿机制，在异常情况下如果未收到事务的提交请求，会调用此方法进行事务状态查询，以此决定是否将事务进行提交/回滚
     * @param msg Check message
     * @return Transaction state
     */
    LocalTransactionState checkLocalTransaction(final MessageExt msg);
}