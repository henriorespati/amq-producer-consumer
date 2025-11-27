package org.demo;

import jakarta.jms.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;

public class MultithreadedProducer {
    // A flag to signal worker threads to stop gracefully
    private static volatile boolean running = true;

    public static void main(String[] args) throws Exception {

        String brokerUrl = "tcp://localhost:61616?useTopologyForLoadBalancing=false";
        if (args.length > 1) {
            brokerUrl = args[1];
        }
        System.out.println("Using broker URL: " + brokerUrl);

        String[] queueNames = createQueues("testQueue", 8);
        int messageCount = 160000; // Total messages to send
        int threads = 8000;        // Number of producer threads

        // Build a reusable 1 MB dummy payload (1024 * 1024 bytes)
        byte[] payload = new byte[1024 * 1024];
        Arrays.fill(payload, (byte) 'X'); // ASCII 'X' bytes

        // Manually manage the factory, do NOT use try-with-resources here
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        factory.setUser("admin");
        factory.setPassword("password");
        factory.setBlockOnDurableSend(true);
        factory.setConfirmationWindowSize(1024 * 1024); // 1 MB

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>(threads);
        AtomicInteger counter = new AtomicInteger(0);

        // Add a shutdown hook to gracefully stop the application
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered. Shutting down gracefully...");
            // Signal threads to stop
            running = false;

            // Shutdown the thread pool
            pool.shutdown();
            try {
                // Wait for existing tasks to terminate
                if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                    System.err.println("Pool did not terminate in 60 seconds. Forcing shutdown...");
                    pool.shutdownNow(); // Cancel currently executing tasks
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // Finally, close the JMS connection factory
            if (factory != null) {
                factory.close();
                System.out.println("JMS Connection Factory closed.");
            }
            System.out.println("Shutdown complete. Total attempted messages: " + (counter.get() - threads));
        }));

        System.out.println("Starting " + threads + " producer threads to send " + messageCount + " messages...");

        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                // Loop until shutdown is triggered or all messages are accounted for
                while (running) {
                    int i = counter.getAndIncrement();
                    if (i > messageCount-1) break;

                    try (Connection connection = factory.createConnection()) {
                        connection.start(); // Connections must be started
                        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        ClientSession coreSession = ((ActiveMQSession) session).getCoreSession();
                        coreSession.setSendAcknowledgementHandler(msg -> {
                            // This may not fire consistently with sync sends, but left for demonstration
                            System.out.println("Send ack on " + Thread.currentThread().getName() + " for idx " + i);
                        });

                        Queue queue = session.createQueue(queueNames[i % queueNames.length]);
                        MessageProducer producer = session.createProducer(queue);
                        BytesMessage msg = session.createBytesMessage();
                        msg.setStringProperty(Message.HDR_DUPLICATE_DETECTION_ID.toString(), UUID.randomUUID().toString());
                        msg.setIntProperty("idx", i);
                        msg.setIntProperty("payloadSizeBytes", payload.length);
                        msg.writeBytes(payload);

                        producer.send(msg);
                        System.out.println("Sent message idx=" + i + " by " + Thread.currentThread().getName());
                    } catch (Exception e) {
                        // Only print if the app is still supposed to be running
                        if (running) {
                            System.err.println("Error sending message idx=" + i + " by " + Thread.currentThread().getName());
                            e.printStackTrace();
                        }
                    }
                }
            }));
        }

        // Wait for all workers to finish their tasks (for a clean, non-Ctrl+C exit)
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("A worker thread encountered an error: " + e.getMessage());
            }
        }

        System.out.println("All producer threads have completed their tasks.");

        // Explicitly trigger the shutdown hook's logic for a normal exit
        // This ensures the factory is closed and the pool is shut down correctly.
        System.exit(0);
    }

    public static String[] createQueues(String prefix, int count) {
        String[] queues = new String[count];
        for (int i = 0; i < count; i++) {
            queues[i] = prefix + (i + 1);
        }
        return queues;
    }


    public static String generateUnique1KB() {
        StringBuilder sb = new StringBuilder(1024);
        
        while (sb.length() < 1024) {
            sb.append(UUID.randomUUID().toString()); // 36 chars
        }
        
        // Trim if slightly over 1024
        if (sb.length() > 1024) {
            sb.setLength(1024);
        }
        
        return sb.toString();
    }

    public static String generateUnique64Bytes() {
        StringBuilder sb = new StringBuilder(64);
        
        // Append UUIDs until we reach 64 bytes
        while (sb.length() < 64) {
            sb.append(UUID.randomUUID().toString().replace("-", "")); // Remove dashes for a tighter fit
        }
        
        // Trim if slightly over 64 bytes
        if (sb.length() > 64) {
            sb.setLength(64);
        }
        
        return sb.toString();
    }
}