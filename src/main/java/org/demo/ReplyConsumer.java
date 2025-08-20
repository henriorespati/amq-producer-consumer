package org.demo;

import jakarta.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import java.util.concurrent.CountDownLatch;

public class ReplyConsumer {
    private static final String brokerURL = "tcp://master.example.com:61616?ConsumerWindowSize=0";
    public static void main(String[] args) throws Exception{
        CountDownLatch latch = new CountDownLatch(1);
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("secret");
        factory.setCallTimeout(5000);

        try (Connection connection = factory.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("testQueue");
            MessageConsumer consumer = session.createConsumer(queue);
            consumer.setMessageListener(message -> {
                try {
                    Queue replyQ = (Queue) message.getJMSReplyTo();  //All messages may endup in DLQ if replyQ is null
                    session.createProducer(replyQ).send(session.createTextMessage("Hello World"));
                    System.out.println(message);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            //session.close();
            latch.await();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
