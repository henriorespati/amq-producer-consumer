package org.demo;

import jakarta.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

import java.util.concurrent.CountDownLatch;

public class ReplyConsumer {
    private static final String brokerURL = "tcp://master.example.com:61616?consumerWindowSize=0";
    public static void main(String[] args) throws Exception{
        CountDownLatch latch = new CountDownLatch(1);
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("secret");
        factory.setCallTimeout(5000);
    
        JmsPoolConnectionFactory poolFactory = new JmsPoolConnectionFactory();
        poolFactory.setConnectionFactory(factory);
        poolFactory.setMaxConnections(10);
        poolFactory.setMaxSessionsPerConnection(50);

        try (Connection connection = poolFactory.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("testQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            consumer.setMessageListener(message -> {
                try {
                    Queue replyQ = (Queue) message.getJMSReplyTo();  // All messages may endup in DLQ if replyQ is null
                    session.createProducer(replyQ).send(
                        session.createTextMessage("Reply to " + ((TextMessage) message).getText())
                    );
                    System.out.println(message);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            poolFactory.stop();
        }

    }
}
