package cn.hjp;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;

public class Consumer {

    public static void main(String[] args) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("hello_world_producer_group");

        consumer.setNamesrvAddr("192.168.3.4:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        try {
        consumer.subscribe("topic_hello_world","");

        consumer.registerMessageListener((List<MessageExt> list, ConsumeConcurrentlyContext context) -> {
            if(list != null) {
                for (MessageExt messageExt : list) {
                    try {
                        System.out.println(new Date() + new String(messageExt.getBody(),"UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

    }
}
