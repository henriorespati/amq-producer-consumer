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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestProducer {
    private static final Logger logger = LoggerFactory.getLogger(RequestProducer.class);
    private static final AtomicInteger sentCounter = new AtomicInteger(0);

    private static final String brokerURL =
            "(tcp://localhost:61617,tcp://localhost:61717)?useTopologyForLoadBalancing=true&sslEnabled=true&trustStoreType=PKCS12&trustStorePath=truststore.p12&trustStorePassword=changeit&verifyHost=false&reconnectAttempts=1&failoverAttempts=1&retryInterval=100";
            // &initialReconnectDelay=1000&maxReconnectAttempts=-1

    private static final String queueName = "testQueue";
    private static final int producerThreads = 2;       // concurrent producer threads
    private static final int requestsPerThread = 100;   // requests per thread

    public static void main(String[] args) throws InterruptedException {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("password");
        factory.setCallTimeout(5000);

        JmsPoolConnectionFactory poolFactory = new JmsPoolConnectionFactory();
        poolFactory.setConnectionFactory(factory);
        poolFactory.setMaxConnections(producerThreads);
        // poolFactory.setMaxSessionsPerConnection(50);

        ExecutorService executor = Executors.newFixedThreadPool(producerThreads);

        for (int i = 0; i < producerThreads; i++) {
            int threadId = i;
            executor.submit(() -> runRequestProducer(poolFactory, threadId));
        }

        // Wait for all producer threads to finish
        executor.shutdown();
        boolean finished = executor.awaitTermination(2, TimeUnit.MINUTES);
        if (finished) {
            logger.info("All request producers finished. Stopping pooled factory...");
            poolFactory.stop();
        } else {
            logger.info("Timeout reached before all request producers finished. Forcing shutdown...");
            executor.shutdownNow();
            poolFactory.stop();
        }

        logger.info("Total messages sent: {}", sentCounter.get());
    }

    private static void runRequestProducer(JmsPoolConnectionFactory poolFactory, int threadId) {
        try (Connection connection = poolFactory.createConnection()) {
            connection.start();

            String brokerUrl = getBrokerUrl(connection);

            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Queue queue = session.createQueue(queueName);
                MessageProducer producer = session.createProducer(queue);
                TemporaryQueue replyQueue = session.createTemporaryQueue();
                MessageConsumer consumer = session.createConsumer(replyQueue);

                for (int i = 1; i <= requestsPerThread; i++) {
                    String text = String.format("Thread-%d request #%d", threadId, i);
                    TextMessage request = session.createTextMessage(text);
                    request.setJMSReplyTo(replyQueue);

                    try {
                        producer.send(request);
                        Message reply = consumer.receive(5000);

                        sentCounter.incrementAndGet();
                        logger.info("[RequestProducer-{}][{}] Sent: {} | Reply: {}", threadId, brokerUrl, text,
                                reply instanceof TextMessage tm ? tm.getText() : reply);
                    } catch (Exception e) {
                        logger.error("[RequestProducer-{}][{}] ERROR sending request: {}", threadId, brokerUrl, e);
                    }
                    
                }

                producer.close();
                consumer.close();
            }

        } catch (Exception e) {
            logger.error("[RequestProducer-{}] ERROR: {}", threadId, e);
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
