package com.example.demo.controller;

import com.example.demo.service.QueueMonitoringService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/actuator/rabbitmq")
@RequiredArgsConstructor
@Slf4j
public class RabbitMQMonitoringController {

    private final QueueMonitoringService queueMonitoringService;

    // Single priority queue
    private static final String MAIN_QUEUE = "inappcommunication.messages-fed";

    /**
     * Get comprehensive RabbitMQ metrics
     */
    @GetMapping("/metrics")
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();

        try {
            // Queue metrics for single priority queue
            QueueMonitoringService.QueueStats queueStats = queueMonitoringService.getQueueStats(MAIN_QUEUE);

            metrics.put("queueName", MAIN_QUEUE);
            metrics.put("queueStats", queueStats);
            metrics.put("totalMessages", queueStats.messageCount());
            metrics.put("totalConsumers", queueStats.consumerCount());
            metrics.put("expectedConsumersPerPod", "50-100");
            metrics.put("minimumConsumers", 50);
            metrics.put("timestamp", System.currentTimeMillis());
            metrics.put("status", queueStats.consumerCount() >= 50 ? "HEALTHY" : "DEGRADED");

        } catch (Exception e) {
            log.error("Failed to get metrics", e);
            metrics.put("error", e.getMessage());
            metrics.put("status", "ERROR");
        }

        return metrics;
    }

    /**
     * Get statistics for a specific queue
     */
    @GetMapping("/queue/{queueName}")
    public QueueMonitoringService.QueueStats getQueueStats(@PathVariable String queueName) {
        return queueMonitoringService.getQueueStats(queueName);
    }

    /**
     * Get queue depth for the main priority queue
     */
    @GetMapping("/queue-depth")
    public Map<String, Object> getQueueDepth() {
        Map<String, Object> result = new HashMap<>();
        try {
            QueueMonitoringService.QueueStats stats = queueMonitoringService.getQueueStats(MAIN_QUEUE);
            result.put("queueName", MAIN_QUEUE);
            result.put("depth", stats.messageCount());
            result.put("status", stats.messageCount() > 10000 ? "OVERLOADED" : "OK");
        } catch (Exception e) {
            log.warn("Failed to get depth for queue {}", MAIN_QUEUE, e);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * Get consumer count for the main priority queue
     */
    @GetMapping("/consumer-count")
    public Map<String, Object> getConsumerCount() {
        Map<String, Object> result = new HashMap<>();
        try {
            QueueMonitoringService.QueueStats stats = queueMonitoringService.getQueueStats(MAIN_QUEUE);
            result.put("queueName", MAIN_QUEUE);
            result.put("consumerCount", stats.consumerCount());
            result.put("expected", "50-100 per pod");
            result.put("status", stats.consumerCount() >= 50 ? "HEALTHY" : "DEGRADED");
        } catch (Exception e) {
            log.warn("Failed to get consumer count for queue {}", MAIN_QUEUE, e);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * Health check endpoint (simplified)
     * Note: Priority distribution cannot be monitored with native priority queues.
     * RabbitMQ handles priority internally, only total depth is visible.
     */
    @GetMapping("/health")
    public Map<String, Object> getHealth() {
        Map<String, Object> health = new HashMap<>();

        try {
            QueueMonitoringService.QueueStats stats = queueMonitoringService.getQueueStats(MAIN_QUEUE);

            int totalConsumers = stats.consumerCount();
            int queueDepth = stats.messageCount();

            String status = "UP";
            if (totalConsumers < 50) {
                status = "DEGRADED";
            }
            if (queueDepth > 10000) {
                status = "OVERLOADED";
            }

            health.put("status", status);
            health.put("queueName", MAIN_QUEUE);
            health.put("totalConsumers", totalConsumers);
            health.put("expectedConsumersPerPod", "50-100");
            health.put("queueDepth", queueDepth);
            health.put("timestamp", System.currentTimeMillis());

        } catch (Exception e) {
            log.error("Health check failed", e);
            health.put("status", "DOWN");
            health.put("error", e.getMessage());
        }

        return health;
    }
}
