package org.demo;

import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncProducer {
    private static final Logger logger = LoggerFactory.getLogger(AsyncProducer.class);
    private static final AtomicInteger sentCounter = new AtomicInteger(0);

    private static final String brokerURL =
            "(tcp://localhost:61617,tcp://localhost:61717)?useTopologyForLoadBalancing=true&blockOnDurableSend=true&blockOnAcknowledge=true&confirmationWindowSize=10000&sslEnabled=true&trustStoreType=PKCS12&trustStorePath=truststore.p12&trustStorePassword=changeit&verifyHost=false&initialReconnectDelay=1000&maxReconnectAttempts=-1";

    private static final String queueName = "testQueue";
    private static final int producerThreads = 4; // concurrent producer threads
    private static final int messagesPerThread = 1; // messages per thread

    public static void main(String[] args) throws InterruptedException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("password");
        factory.setCallTimeout(5000);
        logger.info("ConnectionFactory confirmationWindowSize={} blockOnDurableSend={} blockOnAcknowledge={}", 
            factory.getConfirmationWindowSize(), factory.isBlockOnDurableSend(), factory.isBlockOnAcknowledge());

        ExecutorService executor = Executors.newFixedThreadPool(producerThreads);

        for (int i = 0; i < producerThreads; i++) {
            int threadId = i;
            executor.submit(() -> runProducer(factory, threadId));
        }

        executor.shutdown();
        boolean finished = executor.awaitTermination(2, TimeUnit.MINUTES);
        if (finished) {
            logger.info("All producers finished.");
        } else {
            logger.info("Timeout reached. Forcing shutdown...");
            executor.shutdownNow();
        }

        logger.info("Total messages sent: {}", sentCounter.get());
    }

    private static void runProducer(ActiveMQConnectionFactory factory, int threadId) {
        try (ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection()) {
            connection.start();

            String brokerUrl = getBrokerUrl(connection);

            try (ActiveMQSession session = (ActiveMQSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 MessageProducer producer = session.createProducer(session.createQueue(queueName))) {

                // Set send acknowledgment handler 
                ClientSession coreSession = session.getCoreSession();
                coreSession.setSendAcknowledgementHandler(message -> {
                    sentCounter.incrementAndGet();
                    logger.info("[Producer-{}][{}] Send ack received for message", threadId, brokerUrl);
                });

                for (int i = 1; i <= messagesPerThread; i++) {
                    String text = String.format("Thread-%d message #%d", threadId, i);
                    TextMessage message = session.createTextMessage(text);
                    try {
                        producer.send(message);
                        logger.info("[Producer-{}][{}] Sent: {}", threadId, brokerUrl, text);
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
            if (connection instanceof ActiveMQConnection activeConn) {
                var sf = activeConn.getSessionFactory();
                var rc = sf.getConnection();
                if (rc != null && rc.getTransportConnection() != null) {
                    return rc.getTransportConnection().getRemoteAddress();
                }
            }
        } catch (Exception e) {
            return "error-getting-broker: " + e.getMessage();
        }
        return "unknown-broker";
    }
}
