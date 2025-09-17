package org.demo;

import jakarta.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

public class RequestProducer {
    private static final String brokerURL = "tcp://master.example.com:61616";

    public static void main(String[] args) {

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
            MessageProducer producer = session.createProducer(queue);
            TemporaryQueue replyQueue = session.createTemporaryQueue();
            MessageConsumer consumer = session.createConsumer(replyQueue);

            for (int i = 0; i < 1000; i++) {
                TextMessage request = session.createTextMessage("Hello Server via Artemis!");
                request.setJMSReplyTo(replyQueue);

                producer.send(request);
                System.out.println("Request sent: " + request.getText());

                Message reply = consumer.receive(5000);
                System.out.println(reply);
            }

            producer.close();
            consumer.close();
            session.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            poolFactory.stop();
        }
    }
}
