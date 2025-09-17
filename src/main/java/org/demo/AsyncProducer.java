package org.demo;

import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

public class AsyncProducer {
    private static final String brokerURL = "tcp://master.example.com:61616";

    public static void main(String[] args) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("secret");
        factory.setCallTimeout(5000);
        factory.setBlockOnDurableSend(false);
        factory.setConfirmationWindowSize(10000);
        factory.setReconnectAttempts(3);
        factory.setRetryInterval(1000);
        factory.setRetryIntervalMultiplier(2.0);
        factory.setMaxRetryInterval(10000);

        JmsPoolConnectionFactory poolFactory = new JmsPoolConnectionFactory();
        poolFactory.setConnectionFactory(factory);
        poolFactory.setMaxConnections(10);
        poolFactory.setMaxSessionsPerConnection(50);

        try (Connection connection = poolFactory.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ClientSession clientSession = ((ActiveMQSession) session).getCoreSession();
            clientSession.setSendAcknowledgementHandler(message -> {
                System.out.println("Send ack received");
            });
            MessageProducer producer = session.createProducer(session.createQueue("OneQueue"));
            producer.send(session.createTextMessage("Hello"));
            System.out.println("Message Sent");
            session.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            poolFactory.stop();
        }
    }
}
