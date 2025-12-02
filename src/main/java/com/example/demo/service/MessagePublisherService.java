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
    private final LoadBalancerService loadBalancerService;

    @Value("${app.rabbitmq.exchange.name}")
    private String exchangeName;

    /**
     * Publish message to RabbitMQ with automatic priority assignment and load balancing
     */
    public void publishMessage(String trackingId, MessageRequest request) {
        // Auto-assign priority if not provided (default: MEDIUM)
        MessagePriority priority = request.getPriority() != null ? request.getPriority() : MessagePriority.MEDIUM;

        if (request.getPriority() == null) {
            log.debug("Priority not provided for message {}, auto-assigned to MEDIUM", trackingId);
        }

        // Determine routing key based on priority
        String routingKey = priority.getRoutingKey();

        // Use load balancer to select the best queue from available queues for this priority
        String selectedQueue = loadBalancerService.selectQueue(priority);

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
            // Publish to RabbitMQ
            rabbitTemplate.convertAndSend(
                    exchangeName,
                    routingKey,
                    payload,
                    message -> {
                        // Set message properties
                        message.getMessageProperties().setCorrelationId(trackingId);
                        message.getMessageProperties().setPriority(priority.getPriorityValue());
                        message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                        return message;
                    },
                    correlationData
            );

            // Update tracking with queue name
            trackingService.updateQueueName(trackingId, selectedQueue);

            log.info("Published message {} to exchange {} with routing key {} (selected queue: {}, priority: {})",
                    trackingId, exchangeName, routingKey, selectedQueue, priority);

        } catch (AmqpException e) {
            log.error("Failed to publish message {} to RabbitMQ", trackingId, e);
            trackingService.updateStatus(trackingId, MessageStatus.FAILED, e.getMessage());
            throw new MessageProcessingException("Failed to publish message to RabbitMQ", e);
        }
    }
}
