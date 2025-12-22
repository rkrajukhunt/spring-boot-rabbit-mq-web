package com.example.demo.service;

import com.example.demo.enums.MessagePriority;
import com.example.demo.enums.MessageStatus;
import com.example.demo.exception.MessageProcessingException;
import com.example.demo.model.dto.MessagePayload;
import com.example.demo.model.dto.MessageRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class MessagePublisherService {

    private final RabbitTemplate rabbitTemplate;
    private final TrackingService trackingService;
    private final CommunicationService communicationService;

    @Value("${app.rabbitmq.exchange.name}")
    private String exchangeName;

    @Value("${app.messaging.async.enabled}")
    private boolean asyncEnabled;

    private static final String ROUTING_KEY = "messages.priority";
    private static final String QUEUE_NAME = "inappcommunication.messages-fed";

    /**
     * Publish message to RabbitMQ priority queue with automatic priority assignment
     * Uses RabbitMQ native priority queue (x-max-priority=10)
     *
     * When async is disabled via feature flag, processes synchronously.
     * When RabbitMQ is unavailable, rejects with exception (503 Service Unavailable).
     */
    public void publishMessage(String trackingId, MessageRequest request) {
        // Check feature flag
        if (!asyncEnabled) {
            log.info("Async messaging disabled, processing synchronously for message {}", trackingId);
            processSynchronously(trackingId, request);
            return;
        }

        // Auto-assign priority if not provided (default: MEDIUM)
        MessagePriority priority = request.getPriority() != null ? request.getPriority() : MessagePriority.MEDIUM;

        if (request.getPriority() == null) {
            log.debug("Priority not provided for message {}, auto-assigned to MEDIUM", trackingId);
        }

        // Build message payload
        MessagePayload payload = buildPayload(trackingId, request, priority);

        // Create correlation data for tracking
        CorrelationData correlationData = new CorrelationData(trackingId);

        try {
            // Publish to RabbitMQ priority queue
            rabbitTemplate.convertAndSend(
                    exchangeName,
                    ROUTING_KEY,
                    payload,
                    message -> {
                        // Set message properties including priority (0-10, where 10 is highest)
                        message.getMessageProperties().setCorrelationId(trackingId);
                        message.getMessageProperties().setPriority(priority.getPriorityValue());
                        message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                        return message;
                    },
                    correlationData
            );

            // Update tracking with queue name
            trackingService.updateQueueName(trackingId, QUEUE_NAME);

            log.info("Published message {} to exchange {} with priority {} (value={})",
                    trackingId, exchangeName, priority.name(), priority.getPriorityValue());

        } catch (AmqpException e) {
            // RabbitMQ unavailable - reject request (don't fallback)
            log.error("RabbitMQ unavailable, rejecting request for message {}", trackingId, e);
            trackingService.updateStatus(trackingId, MessageStatus.FAILED,
                    "RabbitMQ unavailable: " + e.getMessage());
            throw new MessageProcessingException("Message queue unavailable", e);
        }
    }

    /**
     * Build message payload from request
     */
    private MessagePayload buildPayload(String trackingId, MessageRequest request, MessagePriority priority) {
        // Convert Map<String, String> to Map<String, Object> for metadata
        Map<String, Object> metadata = null;
        if (request.getMetadata() != null) {
            metadata = new HashMap<>(request.getMetadata());
        }

        // Use provided createRequestId or generate from trackingId
        String createRequestId = request.getCreateRequestId() != null
                ? request.getCreateRequestId()
                : trackingId;

        return MessagePayload.builder()
                .trackingId(trackingId)
                .payload(request.getPayload())
                .priority(priority)
                .retryCount(0)
                .timestamp(LocalDateTime.now())
                .metadata(metadata)
                .createRequestId(createRequestId)
                .build();
    }

    /**
     * Process message synchronously when feature flag is disabled
     * Preserves original synchronous create logic as fallback
     */
    private void processSynchronously(String trackingId, MessageRequest request) {
        MessagePriority priority = request.getPriority() != null ? request.getPriority() : MessagePriority.MEDIUM;
        MessagePayload payload = buildPayload(trackingId, request, priority);

        try {
            // Update status to PROCESSING
            trackingService.updateStatus(trackingId, MessageStatus.PROCESSING, null);

            // Call createCommunication() directly (original synchronous logic)
            communicationService.createCommunication(payload);

            // Update status to COMPLETED
            trackingService.updateStatus(trackingId, MessageStatus.COMPLETED, null);
            trackingService.markCompleted(trackingId);

            log.info("Synchronously processed message {} with priority {}", trackingId, priority.name());

        } catch (Exception e) {
            log.error("Synchronous processing failed for message {}", trackingId, e);
            trackingService.updateStatus(trackingId, MessageStatus.FAILED, e.getMessage());
            throw new MessageProcessingException("Synchronous processing failed", e);
        }
    }
}
