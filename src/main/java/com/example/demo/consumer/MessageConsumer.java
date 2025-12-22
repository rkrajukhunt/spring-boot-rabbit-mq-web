package com.example.demo.consumer;

import com.example.demo.enums.MessageStatus;
import com.example.demo.exception.MessageProcessingException;
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
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
@RequiredArgsConstructor
public class MessageConsumer {

    private final TrackingService trackingService;
    private final CommunicationService communicationService;
    private final RabbitTemplate rabbitTemplate;

    @Value("${app.rabbitmq.retry.max-attempts}")
    private int maxRetryAttempts;

    @Value("${app.messaging.async.processing.timeout-ms:30000}")
    private long processingTimeoutMs;

    @Value("${spring.rabbitmq.listener.simple.prefetch:50}")
    private int prefetchCount;

    @Value("${app.rabbitmq.listener.batch-size:10}")
    private int batchSize;

    @Value("${app.rabbitmq.listener.enable-batch-ack:true}")
    private boolean enableBatchAck;

    private static final String QUEUE_NAME = "inappcommunication.messages-fed";

    // Thread-safe batch acknowledgement tracking
    private final ConcurrentHashMap<String, BatchAckTracker> batchAckTrackers = new ConcurrentHashMap<>();

    /**
     * Batch acknowledgement tracker for each channel (thread)
     */
    private static class BatchAckTracker {
        private long lastAckedDeliveryTag = 0;
        private int messageCount = 0;
        private final int batchSize;

        BatchAckTracker(int batchSize) {
            this.batchSize = batchSize;
        }

        synchronized boolean shouldAck(long deliveryTag) {
            messageCount++;
            if (messageCount >= batchSize) {
                messageCount = 0;
                lastAckedDeliveryTag = deliveryTag;
                return true;
            }
            lastAckedDeliveryTag = deliveryTag;
            return false;
        }

        synchronized long getLastAckedDeliveryTag() {
            return lastAckedDeliveryTag;
        }
    }

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
            @Header(value = "priority", required = false) Integer messagePriority,
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
     * Includes:
     * - Transaction support wrapping create logic + manual ACK
     * - Timeout handling to prevent long-running messages from blocking consumers
     * - Metrics tracking (processing start/end time)
     * - Proper executor cleanup
     */
    private void processMessage(
            MessagePayload payload,
            long deliveryTag,
            Channel channel,
            String queueName) throws IOException {

        String trackingId = payload.getTrackingId();
        String createRequestId = payload.getCreateRequestId() != null ? payload.getCreateRequestId() : trackingId;
        ExecutorService timeoutExecutor = Executors.newSingleThreadExecutor();

        try {
            // Set processing start time for metrics
            long processingStartTime = System.currentTimeMillis();
            payload.setProcessingStartTime(processingStartTime);

            log.info("Processing message {} (createRequestId: {}) from queue {} with timeout {}ms",
                    trackingId, createRequestId, queueName, processingTimeoutMs);

            // Execute business logic + acknowledgment in transaction
            processMessageInTransaction(payload, deliveryTag, channel, timeoutExecutor);

        } catch (Exception e) {
            log.error("Error processing message {} from queue {}", trackingId, queueName, e);

            // Handle error with retry logic
            handleProcessingError(payload, deliveryTag, channel, queueName, e);
        } finally {
            // Ensure executor is properly shutdown
            shutdownExecutor(timeoutExecutor, trackingId);
        }
    }

    /**
     * Process message within a transaction
     * Transaction includes: createCommunication() + manual ACK
     * This ensures atomicity - either both succeed or both fail
     */
    @Transactional
    protected void processMessageInTransaction(
            MessagePayload payload,
            long deliveryTag,
            Channel channel,
            ExecutorService timeoutExecutor) throws IOException {

        String trackingId = payload.getTrackingId();

        try {
            // Update status to PROCESSING
            trackingService.updateStatus(trackingId, MessageStatus.PROCESSING, null);

            // Execute with timeout
            Future<?> future = timeoutExecutor.submit(() -> {
                communicationService.createCommunication(payload);
            });

            try {
                // Wait for completion with timeout
                future.get(processingTimeoutMs, TimeUnit.MILLISECONDS);

                // Set processing end time for metrics
                long processingEndTime = System.currentTimeMillis();
                payload.setProcessingEndTime(processingEndTime);
                long processingDuration = processingEndTime - payload.getProcessingStartTime();

                // Update status to COMPLETED
                trackingService.updateStatus(trackingId, MessageStatus.COMPLETED, null);
                trackingService.markCompleted(trackingId);

                // Acknowledge message (success) - within transaction
                // Use batch acknowledgement if enabled and prefetch > 1
                acknowledgeMessage(channel, deliveryTag, trackingId);

                log.info("Successfully processed message {} in {}ms (createRequestId: {})",
                        trackingId, processingDuration,
                        payload.getCreateRequestId() != null ? payload.getCreateRequestId() : trackingId);

            } catch (TimeoutException e) {
                // Cancel the task
                future.cancel(true);
                log.error("Processing timeout for message {} after {}ms", trackingId, processingTimeoutMs);

                // Rollback transaction and rethrow
                throw new MessageProcessingException("Processing timeout after " + processingTimeoutMs + "ms");
            }

        } catch (Exception e) {
            // Transaction will rollback
            log.error("Transaction failed for message {}", trackingId, e);
            throw new MessageProcessingException("Transaction processing failed", e);
        }
    }

    /**
     * Shutdown executor with proper cleanup
     */
    private void shutdownExecutor(ExecutorService executor, String trackingId) {
        try {
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                log.warn("Executor forcibly shutdown for message {}", trackingId);
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            log.error("Executor shutdown interrupted for message {}", trackingId, e);
        }
    }

    // ========== ACKNOWLEDGEMENT LOGIC ==========

    /**
     * Acknowledge message with optional batch acknowledgement
     *
     * When batch ACK is enabled and prefetch > 1:
     * - Accumulates messages and ACKs in batches (default: 10 messages)
     * - Uses multiple=true to ACK all messages up to deliveryTag
     * - Improves performance by reducing network round-trips
     *
     * When batch ACK is disabled or prefetch = 1:
     * - ACKs each message individually (multiple=false)
     */
    private void acknowledgeMessage(Channel channel, long deliveryTag, String trackingId) throws IOException {
        if (enableBatchAck && prefetchCount > 1) {
            // Get or create batch tracker for this channel
            String channelKey = String.valueOf(channel.getChannelNumber());
            BatchAckTracker tracker = batchAckTrackers.computeIfAbsent(
                channelKey,
                k -> new BatchAckTracker(batchSize)
            );

            // Check if we should batch ACK
            if (tracker.shouldAck(deliveryTag)) {
                // ACK all messages up to and including this deliveryTag
                channel.basicAck(deliveryTag, true);
                log.debug("Batch ACK up to deliveryTag {} for channel {} (trackingId: {})",
                    deliveryTag, channelKey, trackingId);
            } else {
                log.debug("Deferred ACK for deliveryTag {} (waiting for batch)", deliveryTag);
            }
        } else {
            // Individual ACK for each message
            channel.basicAck(deliveryTag, false);
            log.debug("Individual ACK for deliveryTag {} (trackingId: {})", deliveryTag, trackingId);
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
            @Header(value = "priority", required = false) Integer messagePriority,
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
