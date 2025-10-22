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

public class AsyncConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AsyncConsumer.class);
    private static final AtomicInteger receivedCounter = new AtomicInteger(0);

    private static final String brokerURL =
            "(tcp://localhost:61617,tcp://localhost:61717)?useTopologyForLoadBalancing=true&sslEnabled=true&trustStoreType=PKCS12&trustStorePath=truststore.p12&trustStorePassword=changeit&verifyHost=false&reconnectAttempts=1&failoverAttempts=1&retryInterval=100";
            // &initialReconnectDelay=1000&maxReconnectAttempts=-1

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
            int id = i;
            executor.submit(() -> runAsyncConsumer(poolFactory, id, shutdownLatch));
        }

        // Shutdown hook to release latch and stop consumers
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Consumer shutting down...");
            logger.info("Total messages received: {}", receivedCounter.get());
            shutdownLatch.countDown();

            try {
                executor.shutdown();
            } catch (Exception e) {}
            
            try {
                poolFactory.stop();
            } catch (Exception e) {}
        }));

        // Wait indefinitely until shutdown signal
        shutdownLatch.await();
        System.out.println("AsyncConsumer terminated.");
    }

    private static void runAsyncConsumer(JmsPoolConnectionFactory poolFactory, int consumerId, CountDownLatch shutdownLatch) {
        try (Connection connection = poolFactory.createConnection()) {
            connection.start();

            String brokerUrl = getBrokerUrl(connection);
            logger.info("[AsyncConsumer-{}] Connected to broker: {}", consumerId, brokerUrl);

            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));

                consumer.setMessageListener(message -> {
                    if (message instanceof TextMessage textMsg) {
                        try {
                            receivedCounter.incrementAndGet();
                            logger.info("[AsyncConsumer-{}][{}] Received: {}", consumerId, brokerUrl, textMsg.getText());
                        } catch (JMSException e) {
                            logger.error("[AsyncConsumer-{}][{}] ERROR reading message: {}", consumerId, brokerUrl, e);
                            e.printStackTrace();
                        }
                    } else {
                        logger.warn("[AsyncConsumer-{}][{}] Received non-text: {}", consumerId, brokerUrl, message);
                    }
                });

                // Block until shutdown signal
                shutdownLatch.await();
            }

        } catch (InterruptedException e) {
            logger.warn("[AsyncConsumer-{}] Interrupted, stopping consumer.", consumerId);
        } catch (Exception e) {
            logger.error("[AsyncConsumer-{}] ERROR: {}", consumerId, e);
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
