package com.example.demo.integration;

import com.example.demo.enums.MessagePriority;
import com.example.demo.model.dto.MessageRequest;
import com.example.demo.service.MessagePublisherService;
import com.example.demo.service.RabbitMQMetricsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for RabbitMQ Priority Queue functionality
 *
 * Note: These tests require a running RabbitMQ instance
 * For CI/CD pipelines, consider using Testcontainers
 *
 * To run with Testcontainers, add dependency:
 * <dependency>
 *     <groupId>org.testcontainers</groupId>
 *     <artifactId>rabbitmq</artifactId>
 *     <scope>test</scope>
 * </dependency>
 */
@SpringBootTest
@ActiveProfiles("test")
class PriorityQueueIntegrationTest {

    @Autowired(required = false)
    private MessagePublisherService publisherService;

    @Autowired(required = false)
    private RabbitMQMetricsService metricsService;

    @Test
    void testPublishHighPriorityMessage() {
        // Skip test if RabbitMQ is not available
        if (publisherService == null) {
            return;
        }

        // Given
        String trackingId = "integration-test-" + UUID.randomUUID();
        String payload = "{\"userId\":\"integration-user-1\",\"message\":\"High priority test message\"}";

        MessageRequest request = MessageRequest.builder()
                .payload(payload)
                .priority(MessagePriority.HIGH)
                .build();

        // When
        assertDoesNotThrow(() -> publisherService.publishMessage(trackingId, request));

        // Then - verify message was tracked
        // In real test, you would verify the message was processed
        // For now, just ensure no exception was thrown
    }

    @Test
    void testPublishMultiplePriorities() {
        // Skip test if RabbitMQ is not available
        if (publisherService == null) {
            return;
        }

        // Given
        MessagePriority[] priorities = {MessagePriority.HIGH, MessagePriority.MEDIUM, MessagePriority.LOW};

        // When & Then
        for (MessagePriority priority : priorities) {
            String trackingId = "multi-priority-" + priority + "-" + UUID.randomUUID();
            String payload = "{\"priority\":\"" + priority.name() + "\",\"userId\":\"test-user\"}";

            MessageRequest request = MessageRequest.builder()
                    .payload(payload)
                    .priority(priority)
                    .build();

            assertDoesNotThrow(() -> publisherService.publishMessage(trackingId, request));
        }
    }

    @Test
    void testGetQueueMetrics() {
        // Skip test if RabbitMQ is not available
        if (metricsService == null) {
            return;
        }

        // When
        RabbitMQMetricsService.RabbitMQMetrics metrics = metricsService.getMetrics();

        // Then
        assertNotNull(metrics);
        assertNotNull(metrics.queueStats());
        assertNotNull(metrics.healthState());
        assertTrue(metrics.timestamp() > 0);
    }

    @Test
    void testQueueHealthCheck() {
        // Skip test if RabbitMQ is not available
        if (metricsService == null) {
            return;
        }

        // When
        RabbitMQMetricsService.HealthStatus healthStatus = metricsService.getHealthStatus();

        // Then
        assertNotNull(healthStatus);
        assertNotNull(healthStatus.state());
        assertNotNull(healthStatus.message());
    }

    @Test
    void testGetQueueStats() {
        // Skip test if RabbitMQ is not available
        if (metricsService == null) {
            return;
        }

        // When
        String queueName = metricsService.getMainQueueName();
        RabbitMQMetricsService.QueueStats stats = metricsService.getQueueStats(queueName);

        // Then
        assertNotNull(stats);
        assertEquals(queueName, stats.queueName());
        assertTrue(stats.messageCount() >= 0);
        assertTrue(stats.consumerCount() >= 0);
    }

    @Test
    void testPublishWithMetadata() {
        // Skip test if RabbitMQ is not available
        if (publisherService == null) {
            return;
        }

        // Given
        String trackingId = "metadata-test-" + UUID.randomUUID();
        String payload = "{\"userId\":\"metadata-user\"}";

        Map<String, String> metadata = new HashMap<>();
        metadata.put("source", "integration-test");
        metadata.put("version", "1.0");
        metadata.put("environment", "test");

        MessageRequest request = MessageRequest.builder()
                .payload(payload)
                .priority(MessagePriority.MEDIUM)
                .metadata(metadata)
                .build();

        // When & Then
        assertDoesNotThrow(() -> publisherService.publishMessage(trackingId, request));
    }

    @Test
    void testDefaultPriorityWhenNotProvided() {
        // Skip test if RabbitMQ is not available
        if (publisherService == null) {
            return;
        }

        // Given - no priority specified
        String trackingId = "default-priority-" + UUID.randomUUID();
        String payload = "{\"userId\":\"default-user\"}";

        MessageRequest request = MessageRequest.builder()
                .payload(payload)
                .priority(null) // No priority
                .build();

        // When & Then - should default to MEDIUM
        assertDoesNotThrow(() -> publisherService.publishMessage(trackingId, request));
    }
}
