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

    // ========== MAIN QUEUE CONSUMERS ==========

    /**
     * Consumer for Queue 1 (High Priority)
     * Concurrency: 10-20 threads
     */
    @RabbitListener(
            queues = "message-queue-1",
            concurrency = "10-20",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void consumeQueue1(
            @Payload MessagePayload payload,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            Channel channel) throws IOException {

        processMessage(payload, deliveryTag, channel, "message-queue-1");
    }

    /**
     * Consumer for Queue 2 (Medium Priority)
     * Concurrency: 10-20 threads
     */
    @RabbitListener(
            queues = "message-queue-2",
            concurrency = "10-20",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void consumeQueue2(
            @Payload MessagePayload payload,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            Channel channel) throws IOException {

        processMessage(payload, deliveryTag, channel, "message-queue-2");
    }

    /**
     * Consumer for Queue 3 (Low Priority)
     * Concurrency: 10-20 threads
     */
    @RabbitListener(
            queues = "message-queue-3",
            concurrency = "10-20",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void consumeQueue3(
            @Payload MessagePayload payload,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            Channel channel) throws IOException {

        processMessage(payload, deliveryTag, channel, "message-queue-3");
    }

    // ========== MESSAGE PROCESSING LOGIC ==========

    /**
     * Core message processing logic
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
     * Monitor DLQ for Queue 1
     */
    @RabbitListener(queues = "message-queue-1.dlq")
    public void handleDLQ1(@Payload MessagePayload payload, Message message) {
        log.error("Message {} in DLQ for queue-1. Priority: {}, Payload: {}",
                payload.getTrackingId(), payload.getPriority(), payload.getPayload());

        // Optional: Add alerting, special logging, or manual intervention trigger
    }

    /**
     * Monitor DLQ for Queue 2
     */
    @RabbitListener(queues = "message-queue-2.dlq")
    public void handleDLQ2(@Payload MessagePayload payload, Message message) {
        log.error("Message {} in DLQ for queue-2. Priority: {}, Payload: {}",
                payload.getTrackingId(), payload.getPriority(), payload.getPayload());
    }

    /**
     * Monitor DLQ for Queue 3
     */
    @RabbitListener(queues = "message-queue-3.dlq")
    public void handleDLQ3(@Payload MessagePayload payload, Message message) {
        log.error("Message {} in DLQ for queue-3. Priority: {}, Payload: {}",
                payload.getTrackingId(), payload.getPriority(), payload.getPayload());
    }
}
