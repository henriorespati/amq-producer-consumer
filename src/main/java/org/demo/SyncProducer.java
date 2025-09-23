package org.demo;

import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SyncProducer {
    private static final Logger logger = LoggerFactory.getLogger(SyncProducer.class);
    private static final AtomicInteger sentCounter = new AtomicInteger(0);

    private static final String brokerURL = 
        "(tcp://localhost:61617,tcp://localhost:61717)?useTopologyForLoadBalancing=true&sslEnabled=true&trustStoreType=PKCS12&trustStorePath=truststore.p12&trustStorePassword=changeit&verifyHost=false&initialReconnectDelay=1000&maxReconnectAttempts=-1";
    
    private static final String queueName = "testQueue";
    private static final int producerThreads = 4; // number of concurrent producers
    private static final int messagesPerThread = 100; // messages per producer thread

    public static void main(String[] args) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("password");
        factory.setCallTimeout(5000);
        factory.setBlockOnDurableSend(true);
        factory.setBlockOnAcknowledge(true);
        factory.setConfirmationWindowSize(0); // synchronous sends
        factory.setProducerWindowSize(0); // disable producer flow control

        JmsPoolConnectionFactory poolFactory = new JmsPoolConnectionFactory();
        poolFactory.setConnectionFactory(factory);
        poolFactory.setMaxConnections(producerThreads);
        poolFactory.setMaxSessionsPerConnection(10);

        ExecutorService executor = Executors.newFixedThreadPool(producerThreads);

        for (int i = 0; i < producerThreads; i++) {
            int threadId = i;
            executor.submit(() -> runProducer(poolFactory, threadId));
        }

        // Shutdown executor after all tasks finish
        executor.shutdown();
        boolean finished = executor.awaitTermination(1, TimeUnit.MINUTES);
        if (finished) {
            logger.info("All producers finished. Stopping pooled factory...");
            poolFactory.stop();
        } else {
            logger.info("Timeout reached before all producers finished. Forcing shutdown...");
            executor.shutdownNow();
            poolFactory.stop();
        }

        logger.info("Total messages sent: {}", sentCounter.get());
    }

    private static void runProducer(JmsPoolConnectionFactory poolFactory, int threadId) {
        try (Connection connection = poolFactory.createConnection()) {
            connection.start();

            String brokerUrl = getBrokerUrl(connection);
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 MessageProducer producer = session.createProducer(session.createQueue(queueName))) {

                for (int i = 1; i <= messagesPerThread; i++) {
                    String text = String.format("Thread-%d message #%d", threadId, i);
                    TextMessage message = session.createTextMessage(text);
                    try {
                        producer.send(message);
                        sentCounter.incrementAndGet();
                        logger.info("[Producer-{}][{}] Sent: {}", threadId, brokerUrl, text);
                        // Thread.sleep(500); // simulate some delay
                    } catch (Exception e) {
                        logger.error("[Producer-{}][{}] ERROR sending message: {}", threadId, brokerUrl, e);
                    }
                }
            }

        } catch (Exception e) {
            logger.error("[Producer-{}] ERROR: {}", threadId, e);
            e.printStackTrace();
        }
    }

    private static String getBrokerUrl(Connection connection) {
        try {
            if (connection instanceof JmsPoolConnection pooled) {
                Connection delegate = pooled.getConnection();
                if (delegate instanceof ActiveMQConnection activeMQConn) {
                    ClientSessionFactory sf = activeMQConn.getSessionFactory();
                    if (sf != null) {
                        RemotingConnection rc = sf.getConnection();
                        if (rc != null && rc.getTransportConnection() != null) {
                            return rc.getTransportConnection().getRemoteAddress();
                        }
                    }
                }
            }
        } catch (Exception e) {
            return "error-getting-broker: " + e.getMessage();
        }
        return "non-artemis-connection";
    }
}
