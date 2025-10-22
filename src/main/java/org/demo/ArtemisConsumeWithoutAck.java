package org.demo;

import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.api.core.*;

public class ArtemisConsumeWithoutAck {
    public static void main(String[] args) throws Exception {
        String brokerURL = "tcp://localhost:61616?useTopologyForLoadBalancing=false&failoverAttempts=0";
        String username = "admin";
        String password = "password";
        String queueName = "syncQueue";

        ServerLocator locator = ActiveMQClient.createServerLocator(brokerURL);
        ClientSessionFactory factory = locator.createSessionFactory();
        ClientSession session = factory.createSession(username, password, false, false, false, false, 0);

        try {
            // Create queue using constructor (older versions)
            try {
                QueueConfiguration queueConfig = new QueueConfiguration(queueName)
                        .setAddress(queueName)
                        .setDurable(true);
                session.createQueue(queueConfig);
                System.out.println("Queue created: " + queueName);
            } catch (ActiveMQQueueExistsException ignore) {
                System.out.println("Queue already exists: " + queueName);
            }

            // Consume message without ack
            ClientConsumer consumer = session.createConsumer(queueName);
            session.start();

            while (true) {
                ClientMessage message = consumer.receive(1000); // wait 1 sec
                if (message == null) {
                    // No more messages in the queue
                    System.out.println("Queue is empty. Waiting to exit...");
                    break;
                } else {
                    int readable = message.getBodyBuffer().readableBytes();
                    byte[] data = new byte[readable];
                    message.getBodyBuffer().readBytes(data);
                    System.out.println("Received message of " + readable + " bytes");
                    // Do NOT acknowledge, leave for redelivery
                } 
            }

            Thread.sleep(60000); // wait 1 minute before exiting

            try {
                consumer.close();
            } catch (Exception ignore) {}
            try {
                session.close();
            } catch (Exception ignore) {}
        } catch (Exception ignore) {} 
        finally {
            try {
                factory.close();
            } catch (Exception ignore) {}
            try {
                locator.close();
            } catch (Exception ignore) {}
        }
    }
}
