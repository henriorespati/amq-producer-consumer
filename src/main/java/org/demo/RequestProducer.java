package org.demo;

import jakarta.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class RequestProducer {
    private static final String brokerURL = "tcp://master.example.com:61616";
    public static void main(String[] args){

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("secret");
        factory.setCallTimeout(5000);

        try (Connection connection = factory.createConnection()) {
            for(int i =0 ; i < 1000; i++) {
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Queue queue = session.createQueue("testQueue");
                MessageProducer producer = session.createProducer(queue);

                TemporaryQueue replyQueue = session.createTemporaryQueue();
                MessageConsumer consumer = session.createConsumer(replyQueue);
                connection.start();

                TextMessage request = session.createTextMessage("Hello Server via Artemis!");
                request.setJMSReplyTo(replyQueue);

                producer.send(request);
                System.out.println("Request sent with  requestId: " + request.getText());

                Message reply = consumer.receive(5000);

                System.out.println(reply);

                producer.close();
                consumer.close();
                session.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
