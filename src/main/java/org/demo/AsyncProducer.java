package org.demo;

import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AsyncProducer {

    private static final String brokerURL =
            "(tcp://localhost:61617,tcp://localhost:61717)?useTopologyForLoadBalancing=true&sslEnabled=true&trustStoreType=PKCS12&trustStorePath=truststore.p12&trustStorePassword=changeit&verifyHost=false&initialReconnectDelay=1000&maxReconnectAttempts=-1";

    private static final String queueName = "testQueue";
    private static final int producerThreads = 4; // concurrent producer threads
    private static final int messagesPerThread = 100; // messages per thread

    public static void main(String[] args) throws InterruptedException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("password");
        factory.setCallTimeout(5000);
        factory.setBlockOnDurableSend(false);
        factory.setConfirmationWindowSize(10000);

        JmsPoolConnectionFactory poolFactory = new JmsPoolConnectionFactory();
        poolFactory.setConnectionFactory(factory);
        poolFactory.setMaxConnections(producerThreads);
        poolFactory.setMaxSessionsPerConnection(10);

        ExecutorService executor = Executors.newFixedThreadPool(producerThreads);

        for (int i = 0; i < producerThreads; i++) {
            int threadId = i;
            executor.submit(() -> runProducer(poolFactory, threadId));
        }

        executor.shutdown();
        boolean finished = executor.awaitTermination(2, TimeUnit.MINUTES);
        if (finished) {
            System.out.println("All producers finished. Stopping pooled factory...");
            poolFactory.stop();
        } else {
            System.out.println("Timeout reached. Forcing shutdown...");
            executor.shutdownNow();
            poolFactory.stop();
        }
    }

    private static void runProducer(JmsPoolConnectionFactory poolFactory, int threadId) {
        try (Connection connection = poolFactory.createConnection()) {
            connection.start();

            String brokerUrl = getBrokerUrl(connection);

            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 MessageProducer producer = session.createProducer(session.createQueue(queueName))) {

                // Optional: set send acknowledgment handler on underlying CoreSession
                if (session instanceof ActiveMQSession activeSession) {
                    ClientSession coreSession = activeSession.getCoreSession();
                    coreSession.setSendAcknowledgementHandler(message -> {
                        System.out.printf("[Producer-%d][%s] Send ack received for message%n", threadId, brokerUrl);
                    });
                }

                for (int i = 1; i <= messagesPerThread; i++) {
                    String text = String.format("Thread-%d message #%d", threadId, i);
                    TextMessage message = session.createTextMessage(text);
                    producer.send(message);
                    System.out.printf("[Producer-%d][%s] Sent: %s%n", threadId, brokerUrl, text);
                }
            }

        } catch (Exception e) {
            System.err.printf("[Producer-%d] ERROR: %s%n", threadId, e.getMessage());
            e.printStackTrace();
        }
    }

    private static String getBrokerUrl(Connection connection) {
        try {
            if (connection instanceof JmsPoolConnection pooled) {
                Connection delegate = pooled.getConnection();
                if (delegate instanceof ActiveMQConnection activeMQConn) {
                    var sf = activeMQConn.getSessionFactory();
                    var rc = sf.getConnection();
                    if (rc != null && rc.getTransportConnection() != null) {
                        return rc.getTransportConnection().getRemoteAddress();
                    }
                }
            }
        } catch (Exception e) {
            return "error-getting-broker: " + e.getMessage();
        }
        return "non-artemis-connection";
    }
}
