package org.demo;

import jakarta.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class TxConsumer {
    private static final String brokerURL = "tcp://master.example.com:61616";

    public static void main(String[] args) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("secret");

        try (Connection connection = factory.createConnection()) {
            connection.start();

            // transacted = true
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("OneQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            for (int i = 0; i < 5; i++) {
                Message msg = consumer.receive(2000); // wait up to 2 seconds
                if (msg == null) break;
                System.out.println("Received: " + ((TextMessage) msg).getText());
            }

            // Commit → acknowledge all 5 together
            session.commit();
            System.out.println("Transaction committed for batch receive");

            session.close();
        }
    }
}
