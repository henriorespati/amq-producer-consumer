package org.demo;

import jakarta.jms.*;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReplyConsumer {

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
            System.out.println("Shutting down...");
            shutdownLatch.countDown(); // release all consumer threads
            executor.shutdown();
            poolFactory.stop();
        }));

        // Wait indefinitely until shutdown
        shutdownLatch.await();
        System.out.println("ReplyConsumer terminated.");
    }

    private static void runReplyConsumer(JmsPoolConnectionFactory poolFactory, int consumerId, CountDownLatch shutdownLatch) {
        try (Connection connection = poolFactory.createConnection()) {
            connection.start();

            String brokerUrl = getBrokerUrl(connection);
            System.out.printf("[ReplyConsumer-%d] Connected to broker: %s%n", consumerId, brokerUrl);

            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));

                consumer.setMessageListener(message -> {
                    try {
                        Queue replyQ = (Queue) message.getJMSReplyTo();
                        if (replyQ != null) {
                            if (message instanceof TextMessage textMsg) {
                                String replyText = "Reply to " + textMsg.getText();
                                session.createProducer(replyQ).send(session.createTextMessage(replyText));
                                System.out.printf("[ReplyConsumer-%d][%s] Replied: %s%n", consumerId, brokerUrl, replyText);
                            } else {
                                System.out.printf("Non-text message: %s%n", message);                        
                            }
                        } else {
                            System.out.printf("[ReplyConsumer-%d][%s] No reply-to destination, message skipped: %s%n",
                                    consumerId, brokerUrl, message);
                        }
                    } catch (JMSException e) {
                        System.err.printf("[ReplyConsumer-%d][%s] ERROR processing message: %s%n",
                                consumerId, brokerUrl, e.getMessage());
                        e.printStackTrace();
                    }
                });

                // Wait until shutdown signal
                shutdownLatch.await();

            }
        } catch (InterruptedException e) {
            System.out.printf("[ReplyConsumer-%d] Interrupted, stopping consumer.%n", consumerId);
        } catch (Exception e) {
            System.err.printf("[ReplyConsumer-%d] ERROR: %s%n", consumerId, e.getMessage());
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
