package org.demo;

import jakarta.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultithreadedConsumer {

    // Broker URL
    private static final String BROKER_URL = "tcp://localhost:61616?useTopologyForLoadBalancing=false";
    private static final String USER = "admin";
    private static final String PASSWORD = "password";

    public static void main(String[] args) {
        int numQueues = 20;        // Number of queues: testQueue1..testQueue20
        int numThreads = 5;        // Number of consumer threads per queue (customizable)

        ExecutorService executor = Executors.newFixedThreadPool(numQueues * numThreads);

        for (int q = 1; q <= numQueues; q++) {
            String queueName = "testQueue" + q;
            for (int t = 0; t < numThreads; t++) {
                executor.submit(new ConsumerTask(BROKER_URL, USER, PASSWORD, queueName));
            }
        }
    }

    static class ConsumerTask implements Runnable {
        private final String brokerUrl;
        private final String user;
        private final String password;
        private final String queueName;

        public ConsumerTask(String brokerUrl, String user, String password, String queueName) {
            this.brokerUrl = brokerUrl;
            this.user = user;
            this.password = password;
            this.queueName = queueName;
        }

        @Override
        public void run() {
            try (ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl)) {
                connectionFactory.setUser(user);
                connectionFactory.setPassword(password);

                try (Connection connection = connectionFactory.createConnection()) {
                    connection.start();

                    try (Session session = connection.createSession(true, Session.SESSION_TRANSACTED)) {
                        Queue queue = session.createQueue(queueName);
                        try (MessageConsumer consumer = session.createConsumer(queue)) {
                            System.out.println(Thread.currentThread().getName() + " listening on " + queueName);

                            while (true) {
                                Message message = consumer.receive(100);
                                if (message == null) continue;

                                try {
                                    if (message instanceof TextMessage) {
                                        TextMessage textMessage = (TextMessage) message;
                                        System.out.println(Thread.currentThread().getName() +
                                                " Received TextMessage from " + queueName + ": " +
                                                textMessage.getText() +
                                                " | ID: " + message.getJMSMessageID() +
                                                " | Redelivery: " + message.getIntProperty("JMSXDeliveryCount"));
                                    } else if (message instanceof BytesMessage) {
                                        BytesMessage bytesMessage = (BytesMessage) message;
                                        byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
                                        bytesMessage.readBytes(bytes);
                                        System.out.println(Thread.currentThread().getName() +
                                                " Received BytesMessage from " + queueName +
                                                " | ID: " + message.getJMSMessageID() +
                                                " | Redelivery: " + message.getIntProperty("JMSXDeliveryCount"));
                                    }

                                    // Simulate processing delay
                                    // Thread.sleep(500);

                                    // Commit transaction
                                    session.commit();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    session.rollback();
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
