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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
    }

    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    /**
     * asyncPrepareMessage方法中，又调用了TransactionalMessageBridge的asyncPutHalfMessage方法，添加half消息.
     *
     * @param messageInner Prepare(Half) message.
     * @return
     */
    @Override
    public CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner) {
        // 添加half消息
        return transactionalMessageBridge.asyncPutHalfMessage(messageInner);
    }

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    /**
     * 根据half消息的检查次数是否超过最大限制（15次）来决定是否丢弃half消息
     * @param msgExt
     * @param transactionCheckMax
     * @return
     */
    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        // 从属性中获取检查次数
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        // 如果不为空
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            // 如果检查次数大于事务最大的检查次数，表示需要丢弃
            if (checkTime >= transactionCheckMax) {
                return true;
            } else {
                // 检查次数加一
                checkTime++;
            }
        }
        // 更新检查次数
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    /**
     * 根据half消息在队列中的存留时间（72小时）是否超过了最大的保留时间限制来决定是否跳过
     * @param msgExt
     * @return
     */
    private boolean needSkip(MessageExt msgExt) {
        // 计算half消息在队列中的保留时间
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        // 默认72小时就跳过 // 如果大于Broker中设置的最大保留时间，表示需要跳过
        if (valueOfCurrentMinusBorn
            > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
            * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    /**
     * 消息重新加入到了half队列
     *
     * @param msgExt
     * @param offset
     * @return
     */
    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        // 重新将消息入到half消息队列中
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        // 如果加入成功
        if (putMessageResult != null
            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            // 设置消息的逻辑偏移量
            msgExt.setQueueOffset(
                putMessageResult.getAppendMessageResult().getLogicsOffset());
            // 设置消息在CommitLog的偏移量
            msgExt.setCommitLogOffset(
                putMessageResult.getAppendMessageResult().getWroteOffset());
            // 设消息ID
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.debug(
                "Send check message, the offset={} restored in queueOffset={} "
                    + "commitLogOffset={} "
                    + "newMsgId={} realMsgId={} topic={}",
                offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                msgExt.getTopic());
            return true;
        } else {
            // 加入失败
            log.error(
                "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                    + "msgId: {}",
                msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    /**
     *1. 在check方法中会获取half主题（RMQ_SYS_TRANS_HALF_TOPIC）下的所有消息队列，遍历所有的half消息队列，对队列中的half消息进行处理，主要步骤如下：
     *  a. 构建OP队列的消息队列对象MessageQueue
     *      调用getOpQueue获取当前half消息队列对应的OP队列的MessageQueue对象，实际上是创建了一个MessageQueue对象，设置为OP队列的主题
     *      、以及Broker名称和队列的ID，在后面获取消费进度时使用.
     *  b. 获取half队列的消费进度和OP消费队列的消费进度
     *      消费进度的获取是通过调用transactionalMessageBridge的fetchConsumeOffset方法进行查询的，可以看到方法的入参是MessageQueue类型的
     *      ，所以第一步需要构造OP队列的MessageQueue对象，在这一步查询消费进度使用.
     *  c. 从OP队列中拉取消息
     *      调用fillOpRemoveMap方法根据消费进度信息从OP队列中拉取消息，将拉取的消费放入removeMap中，用于判断half消息是否已经处理.
     *  d. 处理每一个half消息
     *     开启while循环，从half队列的消费进度处开始，处理每一个half消息：
     *      1. 如果当前时间减去检查开始时间大于最大处理时间，此时终止循环
     *      2. 如果removeMap中包含当前half消息，表示消息已经被处理，放入到已处理消息集合中doneOpOffset
     *      3. 如果removeMap不包含当前half消息， 调用getHalfMsg方法根据偏移量从half队列获取half消息，如果消息获取不为空继续下一步，否则进行如下处理
     *          a. 判断获取空消息的个数是否大于MAX_RETRY_COUNT_WHEN_HALF_NULL，如果大于将终止本次循环，处理下一个half消息队列
     *          b. 判断拉取消息的状态是否为NO_NEW_MSG，如果是表示队列中没有消息，先终止循环
     *          c. 如果拉取消息的状态是不是NO_NEW_MSG，表示消费进度不合法，获取half消息队列中下一条消息进行处理
     *      4. 调用needDiscard判断是否需要丢弃half消息，或者调用needSkip判断是否需要跳过当前half消息：
     *          a. needDiscard是根据half消息的检查次数是否超过最大限制来决定是否丢弃half消息
     *          b. needSkip是根据half消息在队列中的存留时间是否超过了最大的保留时间限制来决定是否跳过
     *      5. 判断消息的的存入时间是否大于本次开始检查的时间，如果大于说明是新加入的消息，由于事务消息发送后不会立刻提交，所以此时暂不需要进行检查，中断循环即可
     *      6. 计算half消息在队列中的存留时间valueOfCurrentMinusBorn：当前时间 - 消息存入的时间
     *      7. 设置立刻回查事务状态的时间checkImmunityTime：事务的超时时间
     *      8. 从消息属性中获取PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS属性的值放在checkImmunityTimeStr变量中，表示事务的最晚回查时间：
     *          a. 如果checkImmunityTimeStr获取不为空，调用getImmunityTime方法计算事务立刻回查时间，并赋值给checkImmunityTime
     *              ，从代码中可以看出如果checkImmunityTimeStr为-1则返回事务的超时时间，否则返回checkImmunityTimeStr的值并乘以1000转为秒：
     *              1. 计算完checkImmunityTime的值后，判断valueOfCurrentMinusBorn是否小于checkImmunityTime，如果是表明还未到事务的超时时间
     *                  ，此时调用checkPrepareQueueOffset检查half消息在队列中的偏移量，根据检查结果判断是否需要跳过当前消息：
     *                  1. 如果PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET属性获取为空，调用putImmunityMsgBackToHalfQueue将消息重新加入half队列
     *                      ，如果返回true表示加入成功，此时向前推荐消费进度，处理下一条消息，如果加入失败会继续循环处理本条消息（因为进度未向前推进）
     *                  2. 如果PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET属性获取不为空，转为long型，判断OP队列中是否已经包含当前消息的偏移量
     *                      ，如果包含加入到doneOpOffset中并返回true，此时向前推进消费进度，处理下一条消息，否则同样调用putImmunityMsgBackToHalfQueue将消息重新加入half队列，并根据加入成功与否判断是否继续处理下一条消息
     *              总结
     *                  如果事务设置了PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS属性，并且half消息的存留时间小于立刻检查事务的时间，说明还未到时间不需要进行状态检查
     *                  ，此时获取消息在half队列的偏移量，如果获取为空，将消息重新加入到half队列中，如果获取不为空判断是否已经在OP处理队列中，如果返回true处理下一个消息即可
     *                  ，否则同样将消息重新加入half队列中。
     *                      RocketMQ在事务未到最晚回查时间时将消息重新加入了half消息队列，因为加入之后half队列的消费进度会往前推进并在回查结束时更新进度
     *                      ，所以下次检查时并不会检查到旧的half消息。
     *         b. 如果checkImmunityTimeStr获取为空，判断valueOfCurrentMinusBorn（消息存留时间）是否大于等于0并且小于checkImmunityTime（事务超时时间）
     *         ，如果满足条件表示新加入的消息并且还未过事务的超时时间，此时终止循环暂不进行回查，否则进入下一步
     *      9. 判断是否需要进行状态回查isNeedCheck，满足检查的条件为以下三种情况之一：
     *          a. 从OP队列中拉取消息为空并且当前half消息的存留时间已经大于事务设置的最晚回查时间
     *          b. 从OP队列中拉取的消息不为空，并且拉取的最后一条消息的存入时间减去本次开始检查时间大于事务的超时时间
     *          c. half消息在队列中的保留时间小于等于1，说明加入half消息的时间大于本次开始检查的时间
     *      10. 根据isNeedCheck判断是否需要回查
     *          a. 需要回查：调用putBackHalfMsgQueue将half消息重新加入到队列中，如果加入失败继续循环再次处理，如果加入成功调用resolveHalfMsg发送回查请求
     *          b. 不需要回查：调用fillOpRemoveMap继续从OP队列中拉取消息判断
     *      11. 更新i的值，继续处理下一个half消息
     * e. 更新消费进度
     *    主要是更half队列和OP队列的消费进度。
     *    1. 重新添加half消息
     *    2.状态回查请求发送
     *    3.
     *
     *
     *
     *
     *
     *  @param transactionTimeout The minimum time of the transactional message to be checked firstly, one message only
     * exceed this time interval that can be checked.
     * @param transactionCheckMax The maximum number of times the message was checked, if exceed this value, this
     * message will be discarded.
     * @param listener When the message is considered to be checked or discarded, the relative method of this class will
     * be invoked.
     */
    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
        AbstractTransactionalMessageCheckListener listener) {
        try {
            //
            String topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
            //根据half 主题获取消息队列
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.debug("Check topic={}, queues={}", topic, msgQueues);
            // 遍历所有的消息队列
            for (MessageQueue messageQueue : msgQueues) {
                // 获取当前时间做为开始时间
                long startTime = System.currentTimeMillis();
                // 获取当前half消息队列对应的OP队列的MessageQueue对象，实际上是创建了一个MessageQueue对象，设置为OP队列的主题、以及Broker名称和队列的ID
                MessageQueue opQueue = getOpQueue(messageQueue);
                // 获取half队列的消费进度
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);
                // 获取op消息队列的消费进度
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                // 如果消费进度小于0表示不合法
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                        halfOffset, opOffset);
                    continue;
                }

                // 存储已处理的消息
                List<Long> doneOpOffset = new ArrayList<>();
                HashMap<Long, Long> removeMap = new HashMap<>();
                // 根据消费进度从op队列中拉取消息，拉取的消费放入removeMap中，用于判断half消息是否已经处理
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                // 如果拉取消息为空，打印错误继续处理下一个消息队列
                if (null == pullResult) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                        messageQueue, halfOffset, opOffset);
                    continue;
                }
                // single thread
                // 获取消息为空的数量默认为1
                int getMessageNullCount = 1;
                // 新的进度
                long newOffset = halfOffset;
                // 获取half队列的消费进度，赋值给i
                long i = halfOffset;
                // 开启while循环，从half队列的消费进度处开始，处理每一个half消息
                while (true) {
                    // 如果当前时间减去检查开始时间大于最大处理时间，此时终止循环
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    // 如果OP队列中包含当前偏移量，表示消息已经被处理，加入到已处理集合中
                    if (removeMap.containsKey(i)) {
                        log.info("Half offset {} has been committed/rolled back", i);
                        // 如果removeMap中包含当前half消息，表示消息已经被处理，放入到已处理消息集合中doneOpOffset
                        Long removedOpOffset = removeMap.remove(i);
                        // 加入到doneOpOffset集合中
                        doneOpOffset.add(removedOpOffset);
                    } else {// 如果已处理队列中不包含当前消息

                        //如果removeMap不包含当前half消息， 调用getHalfMsg方法根据偏移量从half队列获取half消息，如果消息获取不为空继续下一步，否则进行如下处理
                        // 根据偏移量从half队列获取half消息
                        GetResult getResult = getHalfMsg(messageQueue, i);
                        // 获取消息对象
                        MessageExt msgExt = getResult.getMsg();
                        // 如果获取消息为空
                        if (msgExt == null) {
                            //判断获取空消息的次数是否大于MAX_RETRY_COUNT_WHEN_HALF_NULL，如果大于将终止本次循环，处理下一个half消息队列
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            // 判断拉取消息的状态是否为NO_NEW_MSG，如果是表示队列中没有消息，先终止循环
                            // 判断从half队列获取消息的结果是NO_NEW_MSG，表示没有消息，此时终止循环等待下一次进行检查
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                    messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                // 如果拉取消息的状态是不是NO_NEW_MSG，表示消费进度不合法，获取half消息队列中下一条消息进行处理
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                    i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                // 走到这里说明消息的偏移量不合法，继续获取下一条消息进行处理
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }
                        // 调用needDiscard判断是否需要丢弃half消息，或者调用needSkip判断是否需要跳过当前half消息：
                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            listener.resolveDiscardMsg(msgExt);
                            // 继续处理下一条消息
                            newOffset = i + 1;
                            i++;
                            continue;
                        }
                        // 判断消息的的存入时间是否大于本次开始检查的时间，如果大于说明是新加入的消息，由于事务消息发送后不会立刻提交
                        // ，所以此时暂不需要进行检查，中断循环即可

                        // 如果消息的添加时间是否大于等于本次检查的开始时间，说明是在检查开始之后加入的消息，暂不进行处理
                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.debug("Fresh stored. the miss offset={}, check it later, store={}", i,
                                new Date(msgExt.getStoreTimestamp()));
                            break;
                        }
                        // 计算half消息在队列中的保留时间：当前时间减去消息加入的时间
                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        // 设置立刻回查事务状态的时间（事务的超时时间）
                        long checkImmunityTime = transactionTimeout;
                        // 获取PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS属性，表示事务回查最晚的时间
                        // 下面一个if 逻辑总结：如果事务设置了PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS属性，并且half消息的存留时间小于立刻检查事务的时间
                        // ，说明还未到时间不需要进行状态检查，此时获取消息在half队列的偏移量，如果获取为空，将消息重新加入到half队列中，如果获取不为空判断是否已经在OP处理队列中，如果返回true处理下一个消息即可，否则同样将消息重新加入half队列中。
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        // 如果PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS属性不为空
                        if (null != checkImmunityTimeStr) {
                            // 如果checkImmunityTimeStr获取不为空，调用getImmunityTime方法计算事务立刻回查时间，并赋值给checkImmunityTime，
                            // 从代码中可以看出如果checkImmunityTimeStr为-1则返回事务的超时时间，否则返回checkImmunityTimeStr的值并乘以1000转为秒
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                            // 如果消息的保留时间小于事务回查最晚检查时间
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                // 如果是表明还未到事务的超时时间，此时调用checkPrepareQueueOffset检查half消息在队列中的偏移量，根据检查结果判断是否需要跳过当前消息
                                // 检查half消息在队列中的偏移量，如果返回true跳过本条消息
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt)) {
                                    // 处理下一个消息
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            // 如果checkImmunityTimeStr获取为空，判断valueOfCurrentMinusBorn（消息存留时间）是否大于等于0并且小于checkImmunityTime（事务超时时间）
                            // ，如果满足条件表示新加入的消息并且还未过事务的超时时间，此时终止循环暂不进行回查，否则进入下一步
                            if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                    checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                break;
                            }
                        }
                        // 获取OP消息
                        List<MessageExt> opMsg = pullResult.getMsgFoundList();
                        // 判断是否需要进行状态回查isNeedCheck，满足检查的条件为以下三种情况之一
                        // （1）从OP队列中拉取消息为空并且当前half消息的存留时间已经大于事务设置的最晚回查时间
                        // （2）从OP队列中拉取的消息不为空，并且拉取的最后一条消息的存入时间减去本次开始检查时间大于事务的超时时间
                        // （3）half消息在队列中的保留时间小于等于1，说明加入half消息的时间大于本次开始检查的时间
                        boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)
                            || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout))
                            || (valueOfCurrentMinusBorn <= -1);

                        // 如果需要进行回查
                        if (isNeedCheck) {
                            // 需要回查：调用putBackHalfMsgQueue将half消息重新加入到队列中，如果加入失败继续循环再次处理，如果加入成功调用resolveHalfMsg发送回查请求
                            // 将half消息重新加入到队列中
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            // 向客户端发送事务状态回查的请求，可以看到是通过线程池异步实现的
                            listener.resolveHalfMsg(msgExt);
                        } else {
                            // 不需要回查：调用fillOpRemoveMap继续从OP队列中拉取消息判断
                            pullResult = fillOpRemoveMap(removeMap, opQueue, pullResult.getNextBeginOffset(), halfOffset, doneOpOffset);
                            log.debug("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}", i,
                                messageQueue, pullResult);
                            continue;
                        }
                    }
                    // 更新i的值，继续处理下一个half消息
                    newOffset = i + 1;
                    i++;
                }
                if (newOffset != halfOffset) {
                    // 更新消费进度
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    // 更新处理进度
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
            }
        } catch (Throwable e) {
            log.error("Check error", e);
        }

    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        // 转为long
        checkImmunityTime = getLong(checkImmunityTimeStr);
        // 如果为-1，使用事务的超时时间
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            // 使用checkImmunityTime，乘以1000转为秒
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * Read op message, parse op message, and fill removeMap
     * 从op 队列中拉取消息，并转换消息然后填充到removeMap 中
     *
     *  根据消费进度信息从OP队列中拉取消息，将拉取的消费放入removeMap中，用于判断half消息是否已经处理
     *
     * @param removeMap Half message to be remove, key:halfOffset, value: opOffset.
     * @param opQueue Op message queue.
     * @param pullOffsetOfOp The begin offset of op message queue.
     * @param miniOffset The current minimum offset of half message queue.
     * @param doneOpOffset Stored op messages that have been processed.
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap,
        MessageQueue opQueue, long pullOffsetOfOp, long miniOffset, List<Long> doneOpOffset) {
        // 从OP队列中拉取消息，每次拉取32条
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, 32);
        // 如果拉取为空返回null
        if (null == pullResult) {
            return null;
        }
        // 如果拉取状态为消费进度不合法或者没有匹配的消息
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
            || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            // 从拉取结果中获取消费进度并更新
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            return pullResult;
        }
        // 获取拉取到的消息
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        // 如果为空打印日志
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }
        // 遍历拉取的消息
        for (MessageExt opMessageExt : opMsg) {
            // 获取队列中的偏移量
            Long queueOffset = getLong(new String(opMessageExt.getBody(), TransactionalMessageUtil.charset));
            log.debug("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffset);
            if (TransactionalMessageUtil.REMOVETAG.equals(opMessageExt.getTags())) {
                // 如果偏移量小于最小的偏移量
                if (queueOffset < miniOffset) {
                    // 加入到doneOpOffset中
                    doneOpOffset.add(opMessageExt.getQueueOffset());
                } else {
                    // 加入到已处理消息的集合removeMap中
                    removeMap.put(queueOffset, opMessageExt.getQueueOffset());
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }
        }
        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset,
        MessageExt msgExt) {
        // 从属性中获取消息在half队列的偏移量
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null == prepareQueueOffsetStr) {
            // 如果PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET属性获取为空，调用putImmunityMsgBackToHalfQueue将消息重新加入half队列，
            // 如果返回true表示加入成功，此时向前推荐消费进度，处理下一条消息，如果加入失败会继续循环处理本条消息（因为进度未向前推进）
            return putImmunityMsgBackToHalfQueue(msgExt);
        } else {
            // 如果PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET属性获取不为空，转为long型，判断OP队列中是否已经包含当前消息的偏移量，
            // 如果包含加入到doneOpOffset中并返回true，此时向前推进消费进度，处理下一条消息，
            // 否则同样调用putImmunityMsgBackToHalfQueue将消息重新加入half队列，并根据加入成功与否判断是否继续处理下一条消息
            long prepareQueueOffset = getLong(prepareQueueOffsetStr);
            // 如果为-1，返回false，等待下次循环进行处理
            if (-1 == prepareQueueOffset) {
                return false;
            } else {
                // 如果OP队列中已经包含当前消息的偏移量
                if (removeMap.containsKey(prepareQueueOffset)) {
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    // 加入到已完成的消息集合中
                    doneOpOffset.add(tmpOpOffset);
                    return true;
                } else {
                    // 将消息重新加入half队列
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq Target Message Queue
     * @param offset Offset in the message queue
     * @param nums Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.valueOf(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.valueOf(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        // 获取OP消息队列
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        if (opQueue == null) {
            // 如果获取为空，则创建MessageQueue，主题设置为OP TOPIC，设置Broker名称和队列ID
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                messageQueue.getQueueId());
            // 加入到opQueueMap中
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        getResult.setPullResult(result);
        List<MessageExt> messageExts = result.getMsgFoundList();
        if (messageExts == null) {
            return getResult;
        }
        getResult.setMsg(messageExts.get(0));
        return getResult;
    }

    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    /**
     * 由于CommitLog追加写的性质，RocketMQ并不会直接将half消息从CommitLog中删除，而是使用了另外一个OP主题RMQ_SYS_TRANS_OP_HALF_TOPIC
     * （以下简称OP主题/队列），将已经提交/回滚的消息记录在OP主题队列中。
     * @param msgExt
     * @return
     */
    @Override
    public boolean deletePrepareMessage(MessageExt msgExt) {
        // 添加到OP消息队列
        if (this.transactionalMessageBridge.putOpMessage(msgExt, TransactionalMessageUtil.REMOVETAG)) {
            log.debug("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

}
