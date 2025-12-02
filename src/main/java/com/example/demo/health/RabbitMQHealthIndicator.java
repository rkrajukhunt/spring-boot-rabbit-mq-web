package com.example.demo.health;

import com.example.demo.service.LoadBalancerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class RabbitMQHealthIndicator implements HealthIndicator {

    private final RabbitAdmin rabbitAdmin;
    private final RabbitTemplate rabbitTemplate;
    private final LoadBalancerService loadBalancerService;

    private static final List<String> QUEUE_NAMES = List.of(
            "inappcommunication.priority-high-1-fed",
            "inappcommunication.priority-high-2-fed",
            "inappcommunication.priority-medium-1-fed",
            "inappcommunication.priority-medium-2-fed",
            "inappcommunication.priority-low-1-fed",
            "inappcommunication.priority-low-2-fed"
    );

    private static final int MAX_QUEUE_DEPTH_THRESHOLD = 10000;
    private static final int MIN_TOTAL_CONSUMERS = 60;  // Should be 120 ideally

    @Override
    public Health health() {
        try {
            // Check 1: Connection health
            if (!isConnectionHealthy()) {
                return Health.down()
                        .withDetail("error", "RabbitMQ connection lost")
                        .withDetail("status", "DISCONNECTED")
                        .build();
            }

            // Check 2: Queue depths
            Map<String, Integer> queueDepths = getQueueDepths();
            int maxDepth = queueDepths.values().stream()
                    .max(Integer::compareTo)
                    .orElse(0);

            if (maxDepth > MAX_QUEUE_DEPTH_THRESHOLD) {
                return Health.down()
                        .withDetail("error", "Queue depth exceeds threshold")
                        .withDetail("threshold", MAX_QUEUE_DEPTH_THRESHOLD)
                        .withDetail("maxDepth", maxDepth)
                        .withDetail("queueDepths", queueDepths)
                        .build();
            }

            // Check 3: Consumer counts
            Map<String, Integer> consumerCounts = getConsumerCounts();
            int totalConsumers = consumerCounts.values().stream()
                    .mapToInt(Integer::intValue)
                    .sum();

            if (totalConsumers < MIN_TOTAL_CONSUMERS) {
                return Health.down()
                        .withDetail("error", "Consumer count below threshold")
                        .withDetail("expected", 120)
                        .withDetail("minimum", MIN_TOTAL_CONSUMERS)
                        .withDetail("actual", totalConsumers)
                        .withDetail("consumerCounts", consumerCounts)
                        .build();
            }

            // All checks passed
            return Health.up()
                    .withDetail("connection", "CONNECTED")
                    .withDetail("totalConsumers", totalConsumers)
                    .withDetail("maxQueueDepth", maxDepth)
                    .withDetail("queueDepths", queueDepths)
                    .withDetail("consumerCounts", consumerCounts)
                    .build();

        } catch (Exception e) {
            log.error("Health check failed", e);
            return Health.down()
                    .withException(e)
                    .withDetail("error", "Health check exception: " + e.getMessage())
                    .build();
        }
    }

    /**
     * Check if RabbitMQ connection is healthy
     */
    private boolean isConnectionHealthy() {
        try {
            Boolean result = rabbitTemplate.execute(channel -> channel != null && channel.isOpen());
            return Boolean.TRUE.equals(result);
        } catch (Exception e) {
            log.error("Connection health check failed", e);
            return false;
        }
    }

    /**
     * Get message count (depth) for all queues
     */
    private Map<String, Integer> getQueueDepths() {
        Map<String, Integer> depths = new HashMap<>();
        for (String queueName : QUEUE_NAMES) {
            try {
                LoadBalancerService.QueueStats stats = loadBalancerService.getQueueStats(queueName);
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
    private Map<String, Integer> getConsumerCounts() {
        Map<String, Integer> counts = new HashMap<>();
        for (String queueName : QUEUE_NAMES) {
            try {
                LoadBalancerService.QueueStats stats = loadBalancerService.getQueueStats(queueName);
                counts.put(queueName, stats.consumerCount());
            } catch (Exception e) {
                log.warn("Failed to get consumer count for queue {}: {}", queueName, e.getMessage());
                counts.put(queueName, -1);  // Indicate error
            }
        }
        return counts;
    }
}
