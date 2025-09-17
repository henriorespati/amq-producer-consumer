package org.demo;

import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

public class TxProducer {
    private static final String brokerURL = "tcp://master.example.com:61616";

    public static void main(String[] args) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("secret");
        factory.setCallTimeout(5000);
        factory.setBlockOnDurableSend(true);
        factory.setConfirmationWindowSize(10000);
        factory.setTransactionBatchSize(5);

        JmsPoolConnectionFactory poolFactory = new JmsPoolConnectionFactory();
        poolFactory.setConnectionFactory(factory);
        poolFactory.setMaxConnections(10);
        poolFactory.setMaxSessionsPerConnection(50);

        try (Connection connection = poolFactory.createConnection()) {
            connection.start();

            // transacted = true
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            MessageProducer producer = session.createProducer(session.createQueue("OneQueue"));
            
            // Send multiple messages in the same transaction
            for (int i = 1; i <= 5; i++) {
                TextMessage msg = session.createTextMessage("Message " + i);
                producer.send(msg);
                System.out.println("Sent: " + msg.getText());
            }

            // Commit the transaction
            session.commit();
            System.out.println("Message Sent + Committed");

            session.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
