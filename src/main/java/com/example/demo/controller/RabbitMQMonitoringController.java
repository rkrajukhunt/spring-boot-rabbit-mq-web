package com.example.demo.controller;

import com.example.demo.service.LoadBalancerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/actuator/rabbitmq")
@RequiredArgsConstructor
@Slf4j
public class RabbitMQMonitoringController {

    private final LoadBalancerService loadBalancerService;

    private static final List<String> MAIN_QUEUE_NAMES = List.of(
            "inappcommunication.priority-high-1-fed",
            "inappcommunication.priority-high-2-fed",
            "inappcommunication.priority-medium-1-fed",
            "inappcommunication.priority-medium-2-fed",
            "inappcommunication.priority-low-1-fed",
            "inappcommunication.priority-low-2-fed"
    );

    /**
     * Get comprehensive RabbitMQ metrics
     */
    @GetMapping("/metrics")
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();

        try {
            // Queue metrics
            Map<String, LoadBalancerService.QueueStats> queueStats = getAllQueueMetrics();
            metrics.put("queues", queueStats);

            // Summary statistics
            int totalMessages = queueStats.values().stream()
                    .mapToInt(LoadBalancerService.QueueStats::messageCount)
                    .sum();

            int totalConsumers = queueStats.values().stream()
                    .mapToInt(LoadBalancerService.QueueStats::consumerCount)
                    .sum();

            metrics.put("totalMessages", totalMessages);
            metrics.put("totalConsumers", totalConsumers);
            metrics.put("expectedConsumers", 120);
            metrics.put("timestamp", System.currentTimeMillis());
            metrics.put("status", totalConsumers >= 60 ? "HEALTHY" : "DEGRADED");

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
    public LoadBalancerService.QueueStats getQueueStats(@PathVariable String queueName) {
        return loadBalancerService.getQueueStats(queueName);
    }

    /**
     * Get queue depths for all main queues
     */
    @GetMapping("/queue-depths")
    public Map<String, Integer> getQueueDepths() {
        return MAIN_QUEUE_NAMES.stream()
                .collect(Collectors.toMap(
                        queueName -> queueName,
                        queueName -> {
                            try {
                                return loadBalancerService.getQueueStats(queueName).messageCount();
                            } catch (Exception e) {
                                log.warn("Failed to get depth for queue {}", queueName, e);
                                return -1;
                            }
                        }
                ));
    }

    /**
     * Get consumer counts for all main queues
     */
    @GetMapping("/consumer-counts")
    public Map<String, Integer> getConsumerCounts() {
        return MAIN_QUEUE_NAMES.stream()
                .collect(Collectors.toMap(
                        queueName -> queueName,
                        queueName -> {
                            try {
                                return loadBalancerService.getQueueStats(queueName).consumerCount();
                            } catch (Exception e) {
                                log.warn("Failed to get consumer count for queue {}", queueName, e);
                                return -1;
                            }
                        }
                ));
    }

    /**
     * Get load distribution across priority levels
     */
    @GetMapping("/load-distribution")
    public Map<String, Object> getLoadDistribution() {
        Map<String, Object> distribution = new HashMap<>();

        try {
            Map<String, List<String>> priorityQueues = Map.of(
                    "HIGH", List.of("inappcommunication.priority-high-1-fed", "inappcommunication.priority-high-2-fed"),
                    "MEDIUM", List.of("inappcommunication.priority-medium-1-fed", "inappcommunication.priority-medium-2-fed"),
                    "LOW", List.of("inappcommunication.priority-low-1-fed", "inappcommunication.priority-low-2-fed")
            );

            for (Map.Entry<String, List<String>> entry : priorityQueues.entrySet()) {
                String priority = entry.getKey();
                List<String> queues = entry.getValue();

                int totalMessages = 0;
                int totalConsumers = 0;
                Map<String, Integer> queueDepths = new HashMap<>();

                for (String queueName : queues) {
                    try {
                        LoadBalancerService.QueueStats stats = loadBalancerService.getQueueStats(queueName);
                        totalMessages += stats.messageCount();
                        totalConsumers += stats.consumerCount();
                        queueDepths.put(queueName, stats.messageCount());
                    } catch (Exception e) {
                        log.warn("Failed to get stats for queue {}", queueName, e);
                    }
                }

                Map<String, Object> priorityStats = new HashMap<>();
                priorityStats.put("totalMessages", totalMessages);
                priorityStats.put("totalConsumers", totalConsumers);
                priorityStats.put("queueDepths", queueDepths);
                priorityStats.put("loadBalance", calculateLoadBalance(queueDepths));

                distribution.put(priority, priorityStats);
            }

        } catch (Exception e) {
            log.error("Failed to get load distribution", e);
            distribution.put("error", e.getMessage());
        }

        return distribution;
    }

    /**
     * Health check endpoint (simplified)
     */
    @GetMapping("/health")
    public Map<String, Object> getHealth() {
        Map<String, Object> health = new HashMap<>();

        try {
            int totalConsumers = MAIN_QUEUE_NAMES.stream()
                    .mapToInt(queueName -> {
                        try {
                            return loadBalancerService.getQueueStats(queueName).consumerCount();
                        } catch (Exception e) {
                            return 0;
                        }
                    })
                    .sum();

            int maxDepth = MAIN_QUEUE_NAMES.stream()
                    .mapToInt(queueName -> {
                        try {
                            return loadBalancerService.getQueueStats(queueName).messageCount();
                        } catch (Exception e) {
                            return 0;
                        }
                    })
                    .max()
                    .orElse(0);

            String status = "UP";
            if (totalConsumers < 60) {
                status = "DEGRADED";
            }
            if (maxDepth > 10000) {
                status = "OVERLOADED";
            }

            health.put("status", status);
            health.put("totalConsumers", totalConsumers);
            health.put("expectedConsumers", 120);
            health.put("maxQueueDepth", maxDepth);
            health.put("timestamp", System.currentTimeMillis());

        } catch (Exception e) {
            log.error("Health check failed", e);
            health.put("status", "DOWN");
            health.put("error", e.getMessage());
        }

        return health;
    }

    /**
     * Get all queue metrics
     */
    private Map<String, LoadBalancerService.QueueStats> getAllQueueMetrics() {
        return MAIN_QUEUE_NAMES.stream()
                .collect(Collectors.toMap(
                        queueName -> queueName,
                        loadBalancerService::getQueueStats
                ));
    }

    /**
     * Calculate load balance score (0-100, higher is better)
     * 100 = perfectly balanced, 0 = completely imbalanced
     */
    private int calculateLoadBalance(Map<String, Integer> queueDepths) {
        if (queueDepths.isEmpty()) {
            return 100;
        }

        int maxDepth = queueDepths.values().stream().max(Integer::compareTo).orElse(0);
        int minDepth = queueDepths.values().stream().min(Integer::compareTo).orElse(0);

        if (maxDepth == 0) {
            return 100;  // All queues empty = perfectly balanced
        }

        double difference = maxDepth - minDepth;
        double balance = 1.0 - (difference / maxDepth);
        return (int) (balance * 100);
    }
}
