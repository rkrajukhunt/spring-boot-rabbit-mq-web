package com.example.demo.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.stereotype.Service;

import java.util.Properties;

/**
 * Queue monitoring service for RabbitMQ
 *
 * Note: With native priority queues, load balancing across multiple queues is no longer needed.
 * RabbitMQ automatically distributes messages to all consumers across all pods.
 * This service now provides monitoring capabilities for health checks and metrics.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class QueueMonitoringService {

    private final RabbitAdmin rabbitAdmin;

    /**
     * Get queue statistics for monitoring
     */
    public QueueStats getQueueStats(String queueName) {
        try {
            Properties props = rabbitAdmin.getQueueProperties(queueName);
            if (props != null) {
                Integer messageCount = (Integer) props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
                Integer consumerCount = (Integer) props.get(RabbitAdmin.QUEUE_CONSUMER_COUNT);
                return new QueueStats(queueName, messageCount, consumerCount);
            }
        } catch (Exception e) {
            log.error("Failed to get stats for queue {}", queueName, e);
        }
        return new QueueStats(queueName, 0, 0);
    }

    /**
     * Queue statistics holder
     */
    public record QueueStats(String queueName, int messageCount, int consumerCount) {
    }
}
