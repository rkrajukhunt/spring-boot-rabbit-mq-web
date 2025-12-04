package com.example.demo.controller;

import com.example.demo.service.RabbitMQMetricsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * Custom RabbitMQ Monitoring Controller
 *
 * Provides detailed RabbitMQ metrics endpoints for monitoring and alerting
 * Uses RabbitMQMetricsService for all monitoring logic
 *
 * Endpoints:
 * - GET /actuator/rabbitmq/metrics - Comprehensive metrics
 * - GET /actuator/rabbitmq/health - Health status
 * - GET /actuator/rabbitmq/queue/{queueName} - Specific queue stats
 * - GET /actuator/rabbitmq/queue-depth - Main queue depth
 * - GET /actuator/rabbitmq/consumer-count - Consumer count
 */
@RestController
@RequestMapping("/actuator/rabbitmq")
@RequiredArgsConstructor
@Slf4j
public class RabbitMQMonitoringController {

    private final RabbitMQMetricsService metricsService;

    // JSON response constants
    private static final String KEY_QUEUE_NAME = "queueName";
    private static final String KEY_STATUS = "status";
    private static final String KEY_ERROR = "error";
    private static final String KEY_TIMESTAMP = "timestamp";
    private static final String KEY_MESSAGE_COUNT = "messageCount";
    private static final String KEY_CONSUMER_COUNT = "consumerCount";

    // Status constants
    private static final String STATUS_HEALTHY = "HEALTHY";
    private static final String STATUS_DEGRADED = "DEGRADED";
    private static final String STATUS_ERROR = "ERROR";
    private static final String STATUS_DOWN = "DOWN";
    private static final String STATUS_OK = "OK";
    private static final String STATUS_OVERLOADED = "OVERLOADED";

    /**
     * Get comprehensive RabbitMQ metrics
     * Use for Prometheus/Grafana dashboards
     */
    @GetMapping("/metrics")
    public Map<String, Object> getMetrics() {
        try {
            RabbitMQMetricsService.RabbitMQMetrics metrics = metricsService.getMetrics();
            RabbitMQMetricsService.Thresholds thresholds = metricsService.getThresholds();

            Map<String, Object> response = new HashMap<>();
            response.put(KEY_QUEUE_NAME, metrics.queueStats().queueName());
            response.put(KEY_MESSAGE_COUNT, metrics.queueStats().messageCount());
            response.put(KEY_CONSUMER_COUNT, metrics.queueStats().consumerCount());
            response.put("healthState", metrics.healthState().name());
            response.put("connectionHealthy", metrics.connectionHealthy());
            response.put(KEY_TIMESTAMP, metrics.timestamp());
            response.put("thresholds", Map.of(
                "maxQueueDepth", thresholds.maxQueueDepth(),
                "minConsumers", thresholds.minConsumers(),
                "maxConsumers", thresholds.maxConsumers()
            ));

            return response;

        } catch (Exception e) {
            log.error("Failed to get metrics", e);
            return Map.of(
                KEY_ERROR, e.getMessage(),
                KEY_STATUS, STATUS_ERROR,
                KEY_TIMESTAMP, System.currentTimeMillis()
            );
        }
    }

    /**
     * Get health status
     * Simplified endpoint for load balancer health checks
     */
    @GetMapping("/health")
    public Map<String, Object> getHealth() {
        try {
            RabbitMQMetricsService.HealthStatus healthStatus = metricsService.getHealthStatus();
            RabbitMQMetricsService.QueueStats queueStats = metricsService.getQueueStats(metricsService.getMainQueueName());

            Map<String, Object> response = new HashMap<>();
            response.put(KEY_STATUS, healthStatus.state().name());
            response.put("message", healthStatus.message());
            response.put(KEY_QUEUE_NAME, metricsService.getMainQueueName());
            response.put(KEY_CONSUMER_COUNT, queueStats.consumerCount());
            response.put("queueDepth", queueStats.messageCount());
            response.put("connectionHealthy", metricsService.isConnectionHealthy());
            response.put(KEY_TIMESTAMP, System.currentTimeMillis());

            if (healthStatus.details() != null) {
                response.put("details", healthStatus.details());
            }

            return response;

        } catch (Exception e) {
            log.error("Health check failed", e);
            return Map.of(
                KEY_STATUS, STATUS_DOWN,
                KEY_ERROR, e.getMessage(),
                KEY_TIMESTAMP, System.currentTimeMillis()
            );
        }
    }

    /**
     * Get statistics for a specific queue
     */
    @GetMapping("/queue/{queueName}")
    public Map<String, Object> getQueueStats(@PathVariable String queueName) {
        try {
            RabbitMQMetricsService.QueueStats stats = metricsService.getQueueStats(queueName);
            return Map.of(
                KEY_QUEUE_NAME, stats.queueName(),
                KEY_MESSAGE_COUNT, stats.messageCount(),
                KEY_CONSUMER_COUNT, stats.consumerCount(),
                KEY_TIMESTAMP, System.currentTimeMillis()
            );
        } catch (Exception e) {
            log.error("Failed to get stats for queue {}", queueName, e);
            return Map.of(
                KEY_QUEUE_NAME, queueName,
                KEY_ERROR, e.getMessage(),
                KEY_TIMESTAMP, System.currentTimeMillis()
            );
        }
    }

    /**
     * Get queue depth for the main priority queue
     * Use for alerting on queue buildup
     */
    @GetMapping("/queue-depth")
    public Map<String, Object> getQueueDepth() {
        try {
            RabbitMQMetricsService.QueueStats stats = metricsService.getQueueStats(metricsService.getMainQueueName());
            RabbitMQMetricsService.Thresholds thresholds = metricsService.getThresholds();

            String status = stats.messageCount() > thresholds.maxQueueDepth() ? STATUS_OVERLOADED : STATUS_OK;

            return Map.of(
                KEY_QUEUE_NAME, stats.queueName(),
                "depth", stats.messageCount(),
                "threshold", thresholds.maxQueueDepth(),
                KEY_STATUS, status,
                KEY_TIMESTAMP, System.currentTimeMillis()
            );

        } catch (Exception e) {
            log.error("Failed to get queue depth", e);
            return Map.of(
                KEY_ERROR, e.getMessage(),
                KEY_TIMESTAMP, System.currentTimeMillis()
            );
        }
    }

    /**
     * Get consumer count for the main priority queue
     * Use for monitoring consumer health
     */
    @GetMapping("/consumer-count")
    public Map<String, Object> getConsumerCount() {
        try {
            RabbitMQMetricsService.QueueStats stats = metricsService.getQueueStats(metricsService.getMainQueueName());
            RabbitMQMetricsService.Thresholds thresholds = metricsService.getThresholds();

            String status = stats.consumerCount() >= thresholds.minConsumers() ? STATUS_HEALTHY : STATUS_DEGRADED;

            return Map.of(
                KEY_QUEUE_NAME, stats.queueName(),
                KEY_CONSUMER_COUNT, stats.consumerCount(),
                "expected", thresholds.minConsumers() + "-" + thresholds.maxConsumers() + " per pod",
                "minimum", thresholds.minConsumers(),
                KEY_STATUS, status,
                KEY_TIMESTAMP, System.currentTimeMillis()
            );

        } catch (Exception e) {
            log.error("Failed to get consumer count", e);
            return Map.of(
                KEY_ERROR, e.getMessage(),
                KEY_TIMESTAMP, System.currentTimeMillis()
            );
        }
    }

    /**
     * Get all queue depths
     * Use for dashboard visualization
     */
    @GetMapping("/queue-depths")
    public Map<String, Object> getAllQueueDepths() {
        try {
            Map<String, Integer> depths = metricsService.getQueueDepths();
            return Map.of(
                "queueDepths", depths,
                KEY_TIMESTAMP, System.currentTimeMillis()
            );
        } catch (Exception e) {
            log.error("Failed to get all queue depths", e);
            return Map.of(
                KEY_ERROR, e.getMessage(),
                KEY_TIMESTAMP, System.currentTimeMillis()
            );
        }
    }

    /**
     * Get all consumer counts
     * Use for monitoring consumer distribution
     */
    @GetMapping("/consumer-counts")
    public Map<String, Object> getAllConsumerCounts() {
        try {
            Map<String, Integer> counts = metricsService.getConsumerCounts();
            return Map.of(
                "consumerCounts", counts,
                KEY_TIMESTAMP, System.currentTimeMillis()
            );
        } catch (Exception e) {
            log.error("Failed to get all consumer counts", e);
            return Map.of(
                KEY_ERROR, e.getMessage(),
                KEY_TIMESTAMP, System.currentTimeMillis()
            );
        }
    }
}
