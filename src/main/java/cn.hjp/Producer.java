package cn.hjp;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

public class Producer {

    public static void main(String[] args) {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("hello_world_producer_group");

        defaultMQProducer.setNamesrvAddr("192.168.3.4:9876");

        try {
            defaultMQProducer.start();

            for (int i = 0; i < 100; i++) {
                String msg = "Hello World, this is the " + i + " message";
                Message message = new Message("topic_hello_world", "Tag_hello_world", msg.getBytes("UTF-8"));

                SendResult sendResult = defaultMQProducer.send(message);
                System.out.printf("%s%n",sendResult.toString());
            }
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
