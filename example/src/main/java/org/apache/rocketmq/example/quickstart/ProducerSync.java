package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author: Mr.Harry
 * @date : 2021/4/27 23:15
 * @title : 异步发送消息
 */
public class ProducerSync {

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
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println("异步发送成功：" + sendResult);
                    }

                    @Override
                    public void onException(Throwable e) {
                        System.out.println("异步发送失败：" + e);
                    }
                });
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
