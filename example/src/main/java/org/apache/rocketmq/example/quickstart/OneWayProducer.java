package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author: Mr.Harry
 * @date : 2021/4/27 23:22
 * @title : 发送单向消息
 */
public class OneWayProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {


        DefaultMQProducer producer = new DefaultMQProducer("my-group2");
        producer.setNamesrvAddr("192.168.0.107:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            try {
                Message msg = new Message("TopicTest2" /* Topic */,
                        "Tag2" /* Tag */,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );
                // 只发送，不关注是否发送成功，如日志，等
                producer.sendOneway(msg);
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }
        /*         * Shut down once the producer instance is not longer in use.         */
        producer.shutdown();
    }
}
