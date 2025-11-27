package org.demo;

import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;

public class TxProducer {
    private static final String brokerURL = "tcp://localhost:61616";

    public static void main(String[] args) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("secret");
        factory.setCallTimeout(5000);
        factory.setBlockOnDurableSend(true);
        factory.setConfirmationWindowSize(10000);

        try (Connection connection = factory.createConnection()) {
            connection.start();

            // transacted = true
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            // Get client session to attach send ack handler
            ClientSession clientSession = ((ActiveMQSession) session).getCoreSession();
            clientSession.setSendAcknowledgementHandler(message -> {
                System.out.println("Send ack received");
            });

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
        } finally {
            factory.close();
        }
    }
}
