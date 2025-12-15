package com.example.demo.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Consolidated RabbitMQ Metrics Service
 *
 * Provides comprehensive monitoring capabilities for RabbitMQ:
 * - Queue statistics (message count, consumer count)
 * - Connection health checks
 * - Health status evaluation
 * - Metrics aggregation
 *
 * Used by:
 * - RabbitMQHealthIndicator (Spring Boot Actuator health endpoint)
 * - RabbitMQMonitoringController (Custom monitoring endpoints)
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class RabbitMQMetricsService {

    private final RabbitAdmin rabbitAdmin;
    private final RabbitTemplate rabbitTemplate;

    // Single priority queue configuration
    private static final String MAIN_QUEUE = "inappcommunication.messages-fed";
    private static final List<String> ALL_QUEUES = List.of(MAIN_QUEUE);

    // Thresholds - Read from configuration
    @Value("${app.rabbitmq.monitoring.max-queue-depth:10000}")
    private int maxQueueDepthThreshold;

    @Value("${spring.rabbitmq.listener.simple.concurrency:50}")
    private int minConsumersPerPod;

    @Value("${spring.rabbitmq.listener.simple.max-concurrency:100}")
    private int maxConsumersPerPod;

    /**
     * Get statistics for a specific queue
     */
    public QueueStats getQueueStats(String queueName) {
        try {
            Properties props = rabbitAdmin.getQueueProperties(queueName);
            if (props != null) {
                Integer messageCount = (Integer) props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
                Integer consumerCount = (Integer) props.get(RabbitAdmin.QUEUE_CONSUMER_COUNT);
                return new QueueStats(
                    queueName,
                    messageCount != null ? messageCount : 0,
                    consumerCount != null ? consumerCount : 0
                );
            }
        } catch (Exception e) {
            log.error("Failed to get stats for queue {}", queueName, e);
        }
        return new QueueStats(queueName, 0, 0);
    }

    /**
     * Get statistics for all monitored queues
     */
    public Map<String, QueueStats> getAllQueueStats() {
        Map<String, QueueStats> allStats = new HashMap<>();
        for (String queueName : ALL_QUEUES) {
            allStats.put(queueName, getQueueStats(queueName));
        }
        return allStats;
    }

    /**
     * Check if RabbitMQ connection is healthy
     */
    public boolean isConnectionHealthy() {
        try {
            Boolean result = rabbitTemplate.execute(channel -> channel != null && channel.isOpen());
            return Boolean.TRUE.equals(result);
        } catch (Exception e) {
            log.error("Connection health check failed", e);
            return false;
        }
    }

    /**
     * Get comprehensive health status
     */
    public HealthStatus getHealthStatus() {
        try {
            // Check connection
            if (!isConnectionHealthy()) {
                return new HealthStatus(
                    HealthState.DOWN,
                    "RabbitMQ connection lost",
                    null
                );
            }

            // Get queue stats
            QueueStats mainQueueStats = getQueueStats(MAIN_QUEUE);

            // Check queue depth
            if (mainQueueStats.messageCount() > maxQueueDepthThreshold) {
                return new HealthStatus(
                    HealthState.DEGRADED,
                    "Queue depth exceeds threshold",
                    Map.of(
                        "queueDepth", mainQueueStats.messageCount(),
                        "threshold", maxQueueDepthThreshold,
                        "consumerCount", mainQueueStats.consumerCount()
                    )
                );
            }

            // Check consumer count
            if (mainQueueStats.consumerCount() < minConsumersPerPod) {
                return new HealthStatus(
                    HealthState.DEGRADED,
                    "Consumer count below minimum threshold",
                    Map.of(
                        "actual", mainQueueStats.consumerCount(),
                        "minimum", minConsumersPerPod,
                        "expected", minConsumersPerPod + "-" + maxConsumersPerPod
                    )
                );
            }

            // All checks passed
            return new HealthStatus(
                HealthState.UP,
                "All systems operational",
                Map.of(
                    "queueName", MAIN_QUEUE,
                    "consumerCount", mainQueueStats.consumerCount(),
                    "queueDepth", mainQueueStats.messageCount()
                )
            );

        } catch (Exception e) {
            log.error("Health status check failed", e);
            return new HealthStatus(
                HealthState.DOWN,
                "Health check exception: " + e.getMessage(),
                null
            );
        }
    }

    /**
     * Get comprehensive metrics for monitoring
     */
    public RabbitMQMetrics getMetrics() {
        QueueStats mainQueueStats = getQueueStats(MAIN_QUEUE);
        HealthStatus healthStatus = getHealthStatus();

        return new RabbitMQMetrics(
            mainQueueStats,
            healthStatus.state(),
            isConnectionHealthy(),
            System.currentTimeMillis()
        );
    }

    /**
     * Get message count (depth) for all queues
     */
    public Map<String, Integer> getQueueDepths() {
        Map<String, Integer> depths = new HashMap<>();
        for (String queueName : ALL_QUEUES) {
            try {
                QueueStats stats = getQueueStats(queueName);
                depths.put(queueName, stats.messageCount());
            } catch (Exception e) {
                log.warn("Failed to get depth for queue {}: {}", queueName, e.getMessage());
                depths.put(queueName, -1);  // Indicate error
            }
        }
        return depths;
    }

    /**
     * Get consumer count for all queues
     */
    public Map<String, Integer> getConsumerCounts() {
        Map<String, Integer> counts = new HashMap<>();
        for (String queueName : ALL_QUEUES) {
            try {
                QueueStats stats = getQueueStats(queueName);
                counts.put(queueName, stats.consumerCount());
            } catch (Exception e) {
                log.warn("Failed to get consumer count for queue {}: {}", queueName, e.getMessage());
                counts.put(queueName, -1);  // Indicate error
            }
        }
        return counts;
    }

    /**
     * Get main queue name
     */
    public String getMainQueueName() {
        return MAIN_QUEUE;
    }

    /**
     * Get thresholds for monitoring
     */
    public Thresholds getThresholds() {
        return new Thresholds(
            maxQueueDepthThreshold,
            minConsumersPerPod,
            maxConsumersPerPod
        );
    }

    // ========== RECORD TYPES ==========

    /**
     * Queue statistics holder
     */
    public record QueueStats(
        String queueName,
        int messageCount,
        int consumerCount
    ) {}

    /**
     * Health status holder
     */
    public record HealthStatus(
        HealthState state,
        String message,
        Map<String, Object> details
    ) {}

    /**
     * Comprehensive metrics holder
     */
    public record RabbitMQMetrics(
        QueueStats queueStats,
        HealthState healthState,
        boolean connectionHealthy,
        long timestamp
    ) {}

    /**
     * Thresholds configuration
     */
    public record Thresholds(
        int maxQueueDepth,
        int minConsumers,
        int maxConsumers
    ) {}

    /**
     * Health state enum
     */
    public enum HealthState {
        UP,         // All systems operational
        DEGRADED,   // Operational but with warnings
        DOWN        // System unavailable
    }
}