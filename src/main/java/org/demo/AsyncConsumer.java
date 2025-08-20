package org.demo;

import jakarta.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import java.util.concurrent.CountDownLatch;

public class AsyncConsumer {
    private static final String brokerURL = "tcp://master.example.com:61616";

    public static void main(String[] args) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        CountDownLatch latch = new CountDownLatch(1);
        factory.setUser("admin");
        factory.setPassword("secret");
        factory.setCallTimeout(5000);

        try (Connection connection = factory.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("testQueue");
            MessageConsumer consumer = session.createConsumer(queue);
            consumer.setMessageListener(message -> {
                System.out.println(message);
            });
            //session.close();
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
