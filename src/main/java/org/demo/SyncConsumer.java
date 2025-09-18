package org.demo;

import jakarta.jms.*;
import jakarta.jms.IllegalStateException;

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
import java.util.concurrent.atomic.AtomicInteger;

public class SyncConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SyncConsumer.class);
    private static final AtomicInteger receivedCounter = new AtomicInteger(0);

    private static final String brokerURL =
            "(tcp://localhost:61617,tcp://localhost:61717)?useTopologyForLoadBalancing=true&sslEnabled=true&trustStoreType=PKCS12&trustStorePath=truststore.p12&trustStorePassword=changeit&verifyHost=false&initialReconnectDelay=1000&maxReconnectAttempts=-1";

    private static final String queueName = "testQueue";
    private static final int consumerThreads = 4; // number of parallel consumers

    public static void main(String[] args) {
        // Base factory
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("password");
        factory.setCallTimeout(5000);
        factory.setConsumerWindowSize(0); // disable consumer flow control
        factory.setBlockOnAcknowledge(true);

        // Pooled factory
        JmsPoolConnectionFactory poolFactory = new JmsPoolConnectionFactory();
        poolFactory.setConnectionFactory(factory);
        poolFactory.setMaxConnections(consumerThreads);
        poolFactory.setMaxSessionsPerConnection(8);

        // Thread pool for consumers
        ExecutorService executor = Executors.newFixedThreadPool(consumerThreads);

        for (int i = 0; i < consumerThreads; i++) {
            int id = i;
            executor.submit(() -> runConsumer(poolFactory, id));
        }

        // Shutdown hook for cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Consumer shutting down...");
            logger.info("Total messages received: {}", receivedCounter.get());
            executor.shutdown();
            poolFactory.stop();
        }));
    }

    private static void runConsumer(JmsPoolConnectionFactory poolFactory, int consumerId) {
        try (Connection connection = poolFactory.createConnection()) {
            connection.start();

            String brokerUrl = getBrokerUrl(connection);
            logger.info("[Consumer-{}] Connected to broker: {}", consumerId, brokerUrl);

            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 MessageConsumer consumer = session.createConsumer(session.createQueue(queueName))) {

                while (true) {
                    Message message = consumer.receive(5000);
                    if (message != null) {
                        if (message instanceof TextMessage textMsg) {
                            receivedCounter.incrementAndGet();
                            logger.debug("[Consumer-{}][{}] Received: {}", consumerId, brokerUrl, textMsg.getText());
                        } else {
                            logger.debug("[Consumer-{}][{}] Received non-text: {}", consumerId, brokerUrl, message);
                        }
                    }
                }
            }
        } catch (IllegalStateException e) {
            logger.warn("[Consumer-{}] Connection closed, stopping.", consumerId);
        } catch (Exception e) {
            logger.error("[Consumer-{}] ERROR", consumerId, e);
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
