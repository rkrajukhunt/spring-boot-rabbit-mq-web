package com.example.demo.service;

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

    /**
     * Publish message to RabbitMQ based on priority
     */
    public void publishMessage(String trackingId, MessageRequest request) {
        // Determine routing key and queue name based on priority
        String routingKey = request.getPriority().getRoutingKey();
        String queueName = request.getPriority().getQueueName();

        // Build message payload
        MessagePayload payload = MessagePayload.builder()
                .trackingId(trackingId)
                .payload(request.getPayload())
                .priority(request.getPriority())
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
                        message.getMessageProperties().setPriority(request.getPriority().getPriorityValue());
                        message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                        return message;
                    },
                    correlationData
            );

            // Update tracking with queue name
            trackingService.updateQueueName(trackingId, queueName);

            log.info("Published message {} to exchange {} with routing key {} (queue: {})",
                    trackingId, exchangeName, routingKey, queueName);

        } catch (AmqpException e) {
            log.error("Failed to publish message {} to RabbitMQ", trackingId, e);
            trackingService.updateStatus(trackingId, MessageStatus.FAILED, e.getMessage());
            throw new MessageProcessingException("Failed to publish message to RabbitMQ", e);
        }
    }
}
