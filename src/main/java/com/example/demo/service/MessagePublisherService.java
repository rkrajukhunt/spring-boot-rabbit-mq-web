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

@Service
@Slf4j
@RequiredArgsConstructor
public class MessagePublisherService {

    private final RabbitTemplate rabbitTemplate;
    private final TrackingService trackingService;

    @Value("${app.rabbitmq.exchange.name}")
    private String exchangeName;

    private static final String ROUTING_KEY = "messages.priority";
    private static final String QUEUE_NAME = "inappcommunication.messages-fed";

    /**
     * Publish message to RabbitMQ priority queue with automatic priority assignment
     * Uses RabbitMQ native priority queue (x-max-priority=10)
     */
    public void publishMessage(String trackingId, MessageRequest request) {
        // Auto-assign priority if not provided (default: MEDIUM)
        MessagePriority priority = request.getPriority() != null ? request.getPriority() : MessagePriority.MEDIUM;

        if (request.getPriority() == null) {
            log.debug("Priority not provided for message {}, auto-assigned to MEDIUM", trackingId);
        }

        // Build message payload
        MessagePayload payload = MessagePayload.builder()
                .trackingId(trackingId)
                .payload(request.getPayload())
                .priority(priority)
                .retryCount(0)
                .timestamp(LocalDateTime.now())
                .metadata(request.getMetadata())
                .build();

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
            log.error("Failed to publish message {} to RabbitMQ", trackingId, e);
            trackingService.updateStatus(trackingId, MessageStatus.FAILED, e.getMessage());
            throw new MessageProcessingException("Failed to publish message to RabbitMQ", e);
        }
    }
}
