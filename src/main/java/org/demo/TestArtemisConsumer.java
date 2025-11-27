package org.demo;

import jakarta.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class TestArtemisConsumer {

    public static void main(String[] args) {
        String brokerUrl = "(tcp://localhost:61616,tcp://localhost:61716)?failoverAttempts=3&retryInterval=100&ha=true&useTopologyForLoadBalancing=true";
        // &ha=true&useTopologyForLoadBalancing=false
        // &clientFailureCheckPeriod=500&connectionTTL=1000
        String queueName = "testQueue";

        Connection connection = null;
        Session session = null;
        MessageConsumer consumer = null;

        try (ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl)) {

            connectionFactory.setUser("admin");
            connectionFactory.setPassword("password");
            connection = connectionFactory.createConnection();
            connection.start();

            // session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue(queueName);
            consumer = session.createConsumer(queue);

            System.out.println("Waiting for messages...");

            while (true) {
                Message message = consumer.receive(100); 
                if (message == null) {
                    System.out.println("No message received, continuing...");
                    continue;
                }

                try {
                    if (message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        String text = textMessage.getText();
                        System.out.println("Received TextMessage: " + text + " with ID: " + message.getJMSMessageID() + " redelivery count: " + message.getIntProperty("JMSXDeliveryCount"));
                    } else if (message instanceof BytesMessage) {
                        BytesMessage bytesMessage = (BytesMessage) message;
                        byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
                        bytesMessage.readBytes(bytes); 
                        System.out.println("Received BytesMessage: " + message.getJMSMessageID() + " redelivery count: " + message.getIntProperty("JMSXDeliveryCount"));
                    } else {
                        System.out.println("Received unknown message type.");
                    }
                    
                    // Simulate processing time
                    Thread.sleep(1000);
                } finally {
                    // // Simulate disconnect BEFORE acknowledging
                    // System.out.println("Simulating client disconnect before acknowledge...");
                    // try {
                    //     connection.close(); // forcibly close connection
                    // } catch (JMSException e) {
                    //     e.printStackTrace();
                    // }

                    // // Wait a moment to simulate downtime
                    // Thread.sleep(1000);

                    // // Reconnect
                    // System.out.println("Reconnecting...");
                    // connection = connectionFactory.createConnection();
                    // connection.start();
                    // session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                    // // session = connection.createSession(true, Session.SESSION_TRANSACTED);
                    // queue = session.createQueue(queueName);
                    // consumer = session.createConsumer(queue);
                    // System.out.println("Reconnected successfully.");

                    try {
                        // System.out.println("Rolling back message ID: " + message.getJMSMessageID());
                        // session.rollback();
                        // System.out.println("Rolled back message ID: " + message.getJMSMessageID());

                        // System.out.println("Acknowledging message ID: " + message.getJMSMessageID());
                        // message.acknowledge();
                        // System.out.println("Acknowledged message ID: " + message.getJMSMessageID());

                        System.out.println("Committing message ID: " + message.getJMSMessageID());
                        for (int i = 10; i > 0; i--) {
                            System.out.println(i);
                            Thread.sleep(1000); // Simulate some delay before commit
                        }
                        session.commit();
                        System.out.println("Committed message ID: " + message.getJMSMessageID());
                    } catch (JMSException e) {
                        e.printStackTrace();
                        try {
                            consumer.close();
                            session.close();
                            connection.close();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                        throw new RuntimeException("Failed to acknowledge message", e);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Ensure resources are closed if not already
            try {
                if (consumer != null) consumer.close();
            } catch (Exception ignored) {}
            try {
                if (session != null) session.close();
            } catch (Exception ignored) {}
            try {
                if (connection != null) connection.close();
            } catch (Exception ignored) {}
        }
    }
}
