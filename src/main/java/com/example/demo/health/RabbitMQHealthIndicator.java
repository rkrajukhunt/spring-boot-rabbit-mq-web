package com.example.demo.health;

import com.example.demo.service.RabbitMQMetricsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Spring Boot Actuator Health Indicator for RabbitMQ
 *
 * Provides health status for /actuator/health endpoint
 * Uses RabbitMQMetricsService for all monitoring logic
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class RabbitMQHealthIndicator implements HealthIndicator {

    private final RabbitMQMetricsService metricsService;

    @Override
    public Health health() {
        try {
            // Get health status from metrics service
            RabbitMQMetricsService.HealthStatus healthStatus = metricsService.getHealthStatus();
            Map<String, Integer> queueDepths = metricsService.getQueueDepths();
            Map<String, Integer> consumerCounts = metricsService.getConsumerCounts();
            RabbitMQMetricsService.Thresholds thresholds = metricsService.getThresholds();

            // Build health response based on state
            switch (healthStatus.state()) {
                case UP:
                    return Health.up()
                            .withDetail("status", "CONNECTED")
                            .withDetail("queueName", metricsService.getMainQueueName())
                            .withDetail("queueDepths", queueDepths)
                            .withDetail("consumerCounts", consumerCounts)
                            .withDetail("thresholds", Map.of(
                                "maxQueueDepth", thresholds.maxQueueDepth(),
                                "minConsumers", thresholds.minConsumers(),
                                "maxConsumers", thresholds.maxConsumers()
                            ))
                            .withDetail("message", healthStatus.message())
                            .build();

                case DEGRADED:
                    return Health.down()
                            .withDetail("status", "DEGRADED")
                            .withDetail("error", healthStatus.message())
                            .withDetail("queueDepths", queueDepths)
                            .withDetail("consumerCounts", consumerCounts)
                            .withDetail("details", healthStatus.details())
                            .build();

                case DOWN:
                default:
                    return Health.down()
                            .withDetail("status", "DOWN")
                            .withDetail("error", healthStatus.message())
                            .withDetail("details", healthStatus.details())
                            .build();
            }

        } catch (Exception e) {
            log.error("Health check failed", e);
            return Health.down()
                    .withException(e)
                    .withDetail("error", "Health check exception: " + e.getMessage())
                    .build();
        }
    }
}
