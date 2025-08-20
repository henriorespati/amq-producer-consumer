# AMQ Messaging Examples with Artemis

This project demonstrates three different messaging patterns using the Apache ActiveMQ Artemis broker:

1. Synchronous Producer and Consumer
2. Asynchronous Producer and Consumer
3. Request-Reply Messaging Pattern

## Prerequisites

- Java 17
- Apache ActiveMQ Artemis broker running 
- Maven to build the project 

## Broker Configuration

Make sure your broker is accessible and You may need to adjust host, port, and credentials in the source code.

---
## 1. Synchronous Producer and Consumer

This example demonstrates a simple send-and-receive pattern using blocking calls.

- **Producer** sends a message to a named queue.
- **Consumer** uses `receive()` to synchronously wait for a message.

### Classes:
- `SyncProducer.java`
- `SyncConsumer.java`

---

## 2. Asynchronous Producer and Consumer

This example uses non-blocking message consumption via a `MessageListener`.

- **Producer** sends messages as before.
- **Consumer** uses `setMessageListener()` to receive messages asynchronously.

### Classes:
- `AsyncProducer.java`
- `AsyncConsumer.java`

---

## 3. Request-Reply Messaging Pattern

This example demonstrates a request-reply interaction using temporary queues for responses.

- **Requester** sends a message and waits for a reply.
- **Replier** listens for requests and sends back responses.

### Classes:
- `RequestProducer.java`
- `ReplyConsumer.java`

### ⚠️ Important Note:

> In the **Request-Reply pattern**, if the reply from the consumer becomes `null` (e.g. due to a timeout, crash), all in-flight request messages may end up in the **Dead Letter Queue (DLQ)**.

Make sure the replier is reliably up and handling replies properly to avoid message loss.

---

## Running the Examples

Make sure the Artemis broker is running and properly configured. Then compile and run each example:

```bash
# Compile
mvn clean package
```
