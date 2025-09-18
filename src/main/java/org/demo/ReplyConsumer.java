package org.demo;

import jakarta.jms.*;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplyConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ReplyConsumer.class);
    private static final AtomicInteger sentCounter = new AtomicInteger(0);

    private static final String brokerURL =
            "(tcp://localhost:61617,tcp://localhost:61717)?useTopologyForLoadBalancing=true&sslEnabled=true&trustStoreType=PKCS12&trustStorePath=truststore.p12&trustStorePassword=changeit&verifyHost=false&initialReconnectDelay=1000&maxReconnectAttempts=-1";

    private static final String queueName = "testQueue";
    private static final int consumerThreads = 4; // number of parallel consumers

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("password");
        factory.setCallTimeout(5000);
        factory.setConsumerWindowSize(0);
        factory.setBlockOnAcknowledge(true);

        JmsPoolConnectionFactory poolFactory = new JmsPoolConnectionFactory();
        poolFactory.setConnectionFactory(factory);
        poolFactory.setMaxConnections(consumerThreads);
        poolFactory.setMaxSessionsPerConnection(8);

        ExecutorService executor = Executors.newFixedThreadPool(consumerThreads);

        for (int i = 0; i < consumerThreads; i++) {
            int threadId = i;
            executor.submit(() -> runReplyConsumer(poolFactory, threadId, shutdownLatch));
        }

        // Shutdown hook to release latch and stop threads
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down...");
            shutdownLatch.countDown(); // release all consumer threads
            executor.shutdown();
            poolFactory.stop();
        }));

        // Wait indefinitely until shutdown
        shutdownLatch.await();
        logger.info("ReplyConsumer terminated.");
        logger.info("Total messages sent: {}", sentCounter.get());
    }

    private static void runReplyConsumer(JmsPoolConnectionFactory poolFactory, int consumerId, CountDownLatch shutdownLatch) {
        try (Connection connection = poolFactory.createConnection()) {
            connection.start();

            String brokerUrl = getBrokerUrl(connection);
            logger.info("[ReplyConsumer-{}] Connected to broker: {}", consumerId, brokerUrl);

            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));

                consumer.setMessageListener(message -> {
                    try {
                        Queue replyQ = (Queue) message.getJMSReplyTo();
                        if (replyQ != null) {
                            if (message instanceof TextMessage textMsg) {
                                String replyText = "Reply to " + textMsg.getText();
                                session.createProducer(replyQ).send(session.createTextMessage(replyText));
                                sentCounter.incrementAndGet();
                                logger.debug("[ReplyConsumer-{}][{}] Replied: {}", consumerId, brokerUrl, replyText);
                            } else {
                                logger.warn("[ReplyConsumer-{}][{}] Received non-text: {}", consumerId, brokerUrl, message);                        
                            }
                        } else {
                            logger.warn("[ReplyConsumer-{}][{}] No reply-to destination, message skipped: {}", consumerId, brokerUrl, message);
                        }
                    } catch (JMSException e) {
                        logger.error("[ReplyConsumer-{}][{}] ERROR processing message: {}", consumerId, brokerUrl, e);
                        e.printStackTrace();
                    }
                });

                // Wait until shutdown signal
                shutdownLatch.await();

            }
        } catch (InterruptedException e) {
            logger.warn("[ReplyConsumer-{}] Interrupted, stopping consumer.", consumerId);
        } catch (Exception e) {
            logger.error("[ReplyConsumer-{}] ERROR: {}", consumerId, e);
            e.printStackTrace();
        }
    }

    private static String getBrokerUrl(Connection connection) {
        try {
            if (connection instanceof JmsPoolConnection pooled) {
                Connection delegate = pooled.getConnection();
                if (delegate instanceof ActiveMQConnection activeMQConn) {
                    ClientSessionFactory sf = activeMQConn.getSessionFactory();
                    RemotingConnection rc = sf.getConnection();
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
