package com.example.demo.consumer;

import com.example.demo.enums.MessageStatus;
import com.example.demo.model.dto.MessagePayload;
import com.example.demo.service.CommunicationService;
import com.example.demo.service.TrackingService;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
@RequiredArgsConstructor
public class MessageConsumer {

    private final TrackingService trackingService;
    private final CommunicationService communicationService;
    private final RabbitTemplate rabbitTemplate;

    @Value("${app.rabbitmq.retry.max-attempts}")
    private int maxRetryAttempts;

    // ========== MAIN QUEUE CONSUMERS (6 queues: 2 per priority) ==========

    /**
     * Consumer for High Priority Queue 1
     * Concurrency: 10-20 threads
     */
    @RabbitListener(
            queues = "message-queue-high-1",
            concurrency = "10-20",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void consumeHighQueue1(
            @Payload MessagePayload payload,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            Channel channel) throws IOException {
        processMessage(payload, deliveryTag, channel, "message-queue-high-1");
    }

    /**
     * Consumer for High Priority Queue 2
     * Concurrency: 10-20 threads
     */
    @RabbitListener(
            queues = "message-queue-high-2",
            concurrency = "10-20",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void consumeHighQueue2(
            @Payload MessagePayload payload,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            Channel channel) throws IOException {
        processMessage(payload, deliveryTag, channel, "message-queue-high-2");
    }

    /**
     * Consumer for Medium Priority Queue 1
     * Concurrency: 10-20 threads
     */
    @RabbitListener(
            queues = "message-queue-medium-1",
            concurrency = "10-20",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void consumeMediumQueue1(
            @Payload MessagePayload payload,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            Channel channel) throws IOException {
        processMessage(payload, deliveryTag, channel, "message-queue-medium-1");
    }

    /**
     * Consumer for Medium Priority Queue 2
     * Concurrency: 10-20 threads
     */
    @RabbitListener(
            queues = "message-queue-medium-2",
            concurrency = "10-20",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void consumeMediumQueue2(
            @Payload MessagePayload payload,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            Channel channel) throws IOException {
        processMessage(payload, deliveryTag, channel, "message-queue-medium-2");
    }

    /**
     * Consumer for Low Priority Queue 1
     * Concurrency: 10-20 threads
     */
    @RabbitListener(
            queues = "message-queue-low-1",
            concurrency = "10-20",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void consumeLowQueue1(
            @Payload MessagePayload payload,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            Channel channel) throws IOException {
        processMessage(payload, deliveryTag, channel, "message-queue-low-1");
    }

    /**
     * Consumer for Low Priority Queue 2
     * Concurrency: 10-20 threads
     */
    @RabbitListener(
            queues = "message-queue-low-2",
            concurrency = "10-20",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void consumeLowQueue2(
            @Payload MessagePayload payload,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            Channel channel) throws IOException {
        processMessage(payload, deliveryTag, channel, "message-queue-low-2");
    }

    // ========== MESSAGE PROCESSING LOGIC ==========

    /**
     * Core message processing logic (shared by all consumers)
     */
    private void processMessage(
            MessagePayload payload,
            long deliveryTag,
            Channel channel,
            String queueName) throws IOException {

        String trackingId = payload.getTrackingId();

        try {
            log.info("Processing message {} from queue {}", trackingId, queueName);

            // Update status to PROCESSING
            trackingService.updateStatus(trackingId, MessageStatus.PROCESSING, null);

            // Call the heavy business logic (createCommunication)
            communicationService.createCommunication(payload);

            // Update status to COMPLETED
            trackingService.updateStatus(trackingId, MessageStatus.COMPLETED, null);
            trackingService.markCompleted(trackingId);

            // Acknowledge message (success)
            channel.basicAck(deliveryTag, false);

            log.info("Successfully processed message {} from queue {}", trackingId, queueName);

        } catch (Exception e) {
            log.error("Error processing message {} from queue {}", trackingId, queueName, e);

            // Handle error with retry logic
            handleProcessingError(payload, deliveryTag, channel, queueName, e);
        }
    }

    // ========== RETRY LOGIC ==========

    /**
     * Handle processing errors with exponential backoff retry mechanism
     */
    private void handleProcessingError(
            MessagePayload payload,
            long deliveryTag,
            Channel channel,
            String queueName,
            Exception error) throws IOException {

        String trackingId = payload.getTrackingId();
        int currentRetry = payload.getRetryCount();

        if (currentRetry < maxRetryAttempts) {
            // Send to retry queue
            int nextRetry = currentRetry + 1;
            payload.setRetryCount(nextRetry);

            String retryQueue = queueName + ".retry." + nextRetry;

            log.warn("Message {} failed, attempt {}/{}. Sending to retry queue: {}",
                    trackingId, nextRetry, maxRetryAttempts, retryQueue);

            // Publish to retry queue
            rabbitTemplate.convertAndSend(retryQueue, payload);

            // Acknowledge original message (won't be reprocessed)
            channel.basicAck(deliveryTag, false);

            // Update tracking
            trackingService.updateStatus(trackingId, MessageStatus.RETRY, error.getMessage());
            trackingService.incrementRetryCount(trackingId);

        } else {
            // Max retries exhausted - send to DLQ
            String dlqQueue = queueName + ".dlq";

            log.error("Message {} exhausted all {} retries. Sending to DLQ: {}",
                    trackingId, maxRetryAttempts, dlqQueue);

            // Publish to DLQ
            rabbitTemplate.convertAndSend(dlqQueue, payload);

            // Acknowledge original message
            channel.basicAck(deliveryTag, false);

            // Update tracking
            trackingService.updateStatus(
                    trackingId,
                    MessageStatus.DEAD_LETTER,
                    "Max retries exhausted: " + error.getMessage()
            );
        }
    }

    // ========== DEAD LETTER QUEUE LISTENERS (Monitoring) ==========

    /**
     * Monitor DLQ for High Priority Queue 1
     */
    @RabbitListener(queues = "message-queue-high-1.dlq")
    public void handleDLQHigh1(@Payload MessagePayload payload, Message message) {
        logDLQMessage(payload, "message-queue-high-1");
    }

    /**
     * Monitor DLQ for High Priority Queue 2
     */
    @RabbitListener(queues = "message-queue-high-2.dlq")
    public void handleDLQHigh2(@Payload MessagePayload payload, Message message) {
        logDLQMessage(payload, "message-queue-high-2");
    }

    /**
     * Monitor DLQ for Medium Priority Queue 1
     */
    @RabbitListener(queues = "message-queue-medium-1.dlq")
    public void handleDLQMedium1(@Payload MessagePayload payload, Message message) {
        logDLQMessage(payload, "message-queue-medium-1");
    }

    /**
     * Monitor DLQ for Medium Priority Queue 2
     */
    @RabbitListener(queues = "message-queue-medium-2.dlq")
    public void handleDLQMedium2(@Payload MessagePayload payload, Message message) {
        logDLQMessage(payload, "message-queue-medium-2");
    }

    /**
     * Monitor DLQ for Low Priority Queue 1
     */
    @RabbitListener(queues = "message-queue-low-1.dlq")
    public void handleDLQLow1(@Payload MessagePayload payload, Message message) {
        logDLQMessage(payload, "message-queue-low-1");
    }

    /**
     * Monitor DLQ for Low Priority Queue 2
     */
    @RabbitListener(queues = "message-queue-low-2.dlq")
    public void handleDLQLow2(@Payload MessagePayload payload, Message message) {
        logDLQMessage(payload, "message-queue-low-2");
    }

    /**
     * Helper method to log DLQ messages
     */
    private void logDLQMessage(MessagePayload payload, String sourceQueue) {
        log.error("Message {} in DLQ for {}. Priority: {}, RetryCount: {}, Payload: {}",
                payload.getTrackingId(), sourceQueue, payload.getPriority(),
                payload.getRetryCount(), payload.getPayload());

        // Optional: Add alerting, special logging, or manual intervention trigger
        // Example: send email, trigger PagerDuty, write to special audit table
    }
}
