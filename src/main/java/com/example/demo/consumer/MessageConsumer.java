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

    private static final String QUEUE_NAME = "inappcommunication.messages-fed";

    // ========== MAIN PRIORITY QUEUE CONSUMER ==========

    /**
     * Consumer for the single priority queue with RabbitMQ native priority support
     *
     * Concurrency: 50-100 threads (scalable based on load)
     * RabbitMQ automatically processes HIGH priority messages first
     *
     * Multi-Pod Architecture:
     * - Each pod runs 50-100 consumer threads
     * - All pods compete for messages from the same queue
     * - RabbitMQ distributes messages across all consumers from all pods
     * - Priority ordering is maintained: HIGH messages delivered first
     *
     * Horizontal Scaling:
     * - 1 pod  = 50-100 consumers  = ~10M msgs/day (1s processing)
     * - 3 pods = 150-300 consumers = ~30M msgs/day (1s processing)
     * - N pods = N × 50-100 consumers (linear scaling)
     */
    @RabbitListener(
            queues = QUEUE_NAME,
            concurrency = "50-100",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void consumePriorityQueue(
            @Payload MessagePayload payload,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            @Header(value = AmqpHeaders.PRIORITY, required = false) Integer messagePriority,
            Channel channel) throws IOException {

        // Log message priority for monitoring
        if (messagePriority != null) {
            log.debug("Processing message {} with priority {}", payload.getTrackingId(), messagePriority);
        }

        processMessage(payload, deliveryTag, channel, QUEUE_NAME);
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

            // Retry queue name: inappcommunication.messages-retry-{1,2,3}-fed
            String retryQueue = "inappcommunication.messages-retry-" + nextRetry + "-fed";

            log.warn("Message {} failed, attempt {}/{}. Sending to retry queue: {}",
                    trackingId, nextRetry, maxRetryAttempts, retryQueue);

            // Publish to retry queue with same priority
            rabbitTemplate.convertAndSend(retryQueue, payload, message -> {
                // Preserve original priority during retry
                message.getMessageProperties().setPriority(payload.getPriority().getPriorityValue());
                return message;
            });

            // Acknowledge original message (won't be reprocessed)
            channel.basicAck(deliveryTag, false);

            // Update tracking
            trackingService.updateStatus(trackingId, MessageStatus.RETRY, error.getMessage());
            trackingService.incrementRetryCount(trackingId);

        } else {
            // Max retries exhausted - send to DLQ
            String dlqQueue = "inappcommunication.messages-dlq-fed";

            log.error("Message {} exhausted all {} retries. Sending to DLQ: {}",
                    trackingId, maxRetryAttempts, dlqQueue);

            // Publish to DLQ with original priority for analysis
            rabbitTemplate.convertAndSend(dlqQueue, payload, message -> {
                // Preserve original priority in DLQ for analysis
                message.getMessageProperties().setPriority(payload.getPriority().getPriorityValue());
                return message;
            });

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

    // ========== DEAD LETTER QUEUE LISTENER (Monitoring) ==========

    /**
     * Monitor DLQ for messages that exhausted all retries
     * Logs failed messages for manual investigation and potential replay
     */
    @RabbitListener(queues = "inappcommunication.messages-dlq-fed")
    public void handleDLQ(
            @Payload MessagePayload payload,
            @Header(value = AmqpHeaders.PRIORITY, required = false) Integer messagePriority,
            Message message) {

        log.error("Message {} in DLQ. Priority: {} (value={}), RetryCount: {}, Payload: {}",
                payload.getTrackingId(),
                payload.getPriority(),
                messagePriority,
                payload.getRetryCount(),
                payload.getPayload());

        // Optional: Add alerting, special logging, or manual intervention trigger
        // Examples:
        // - Send email/Slack notification
        // - Trigger PagerDuty alert for HIGH priority failures
        // - Write to special audit table for compliance
        // - Store in dead letter storage for replay mechanism
    }
}
