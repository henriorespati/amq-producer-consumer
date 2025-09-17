package org.demo;

import jakarta.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

import java.util.concurrent.CountDownLatch;

public class AsyncConsumer {
    private static final String brokerURL = "tcp://master.example.com:61616";

    public static void main(String[] args) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        CountDownLatch latch = new CountDownLatch(1);
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
                System.out.println(message);
            });

            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
