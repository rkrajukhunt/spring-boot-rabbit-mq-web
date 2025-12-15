package com.example.demo.service;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RabbitMQMetricsServiceTest {

    @Mock
    private RabbitAdmin rabbitAdmin;

    @Mock
    private RabbitTemplate rabbitTemplate;

    @InjectMocks
    private RabbitMQMetricsService metricsService;

    private static final String MAIN_QUEUE = "inappcommunication.messages-fed";

    @BeforeEach
    void setUp() {
        // Setup common mocks
    }

    @Test
    void testGetQueueStats_Success() {
        // Given
        Properties props = new Properties();
        props.put(RabbitAdmin.QUEUE_MESSAGE_COUNT, 100);
        props.put(RabbitAdmin.QUEUE_CONSUMER_COUNT, 75);

        when(rabbitAdmin.getQueueProperties(MAIN_QUEUE)).thenReturn(props);

        // When
        RabbitMQMetricsService.QueueStats stats = metricsService.getQueueStats(MAIN_QUEUE);

        // Then
        assertNotNull(stats);
        assertEquals(MAIN_QUEUE, stats.queueName());
        assertEquals(100, stats.messageCount());
        assertEquals(75, stats.consumerCount());
    }

    @Test
    void testGetQueueStats_NullProperties_ReturnsZeros() {
        // Given
        when(rabbitAdmin.getQueueProperties(MAIN_QUEUE)).thenReturn(null);

        // When
        RabbitMQMetricsService.QueueStats stats = metricsService.getQueueStats(MAIN_QUEUE);

        // Then
        assertNotNull(stats);
        assertEquals(MAIN_QUEUE, stats.queueName());
        assertEquals(0, stats.messageCount());
        assertEquals(0, stats.consumerCount());
    }

    @Test
    void testGetQueueStats_ExceptionThrown_ReturnsZeros() {
        // Given
        when(rabbitAdmin.getQueueProperties(MAIN_QUEUE))
                .thenThrow(new RuntimeException("RabbitMQ connection error"));

        // When
        RabbitMQMetricsService.QueueStats stats = metricsService.getQueueStats(MAIN_QUEUE);

        // Then
        assertNotNull(stats);
        assertEquals(0, stats.messageCount());
        assertEquals(0, stats.consumerCount());
    }

    @Test
    void testGetQueueStats_NullMessageCount_HandledGracefully() {
        // Given
        Properties props = new Properties();
        props.put(RabbitAdmin.QUEUE_CONSUMER_COUNT, 50);
        // messageCount is null

        when(rabbitAdmin.getQueueProperties(MAIN_QUEUE)).thenReturn(props);

        // When
        RabbitMQMetricsService.QueueStats stats = metricsService.getQueueStats(MAIN_QUEUE);

        // Then
        assertEquals(0, stats.messageCount());
        assertEquals(50, stats.consumerCount());
    }

    @Test
    void testGetAllQueueStats_Success() {
        // Given
        Properties props = new Properties();
        props.put(RabbitAdmin.QUEUE_MESSAGE_COUNT, 500);
        props.put(RabbitAdmin.QUEUE_CONSUMER_COUNT, 80);

        when(rabbitAdmin.getQueueProperties(MAIN_QUEUE)).thenReturn(props);

        // When
        Map<String, RabbitMQMetricsService.QueueStats> allStats = metricsService.getAllQueueStats();

        // Then
        assertNotNull(allStats);
        assertEquals(1, allStats.size());
        assertTrue(allStats.containsKey(MAIN_QUEUE));

        RabbitMQMetricsService.QueueStats stats = allStats.get(MAIN_QUEUE);
        assertEquals(500, stats.messageCount());
        assertEquals(80, stats.consumerCount());
    }

    @Test
    void testIsConnectionHealthy_ChannelOpen_ReturnsTrue() {
        // Given
        when(rabbitTemplate.execute(any())).thenAnswer(invocation -> {
            org.springframework.amqp.rabbit.connection.ConnectionFactory.ChannelCallback<?> callback =
                    invocation.getArgument(0);
            Channel mockChannel = mock(Channel.class);
            when(mockChannel.isOpen()).thenReturn(true);
            return callback.doInRabbit(mockChannel);
        });

        // When
        boolean healthy = metricsService.isConnectionHealthy();

        // Then
        assertTrue(healthy);
    }

    @Test
    void testIsConnectionHealthy_ChannelClosed_ReturnsFalse() {
        // Given
        when(rabbitTemplate.execute(any())).thenAnswer(invocation -> {
            org.springframework.amqp.rabbit.connection.ConnectionFactory.ChannelCallback<?> callback =
                    invocation.getArgument(0);
            Channel mockChannel = mock(Channel.class);
            when(mockChannel.isOpen()).thenReturn(false);
            return callback.doInRabbit(mockChannel);
        });

        // When
        boolean healthy = metricsService.isConnectionHealthy();

        // Then
        assertFalse(healthy);
    }

    @Test
    void testIsConnectionHealthy_ExceptionThrown_ReturnsFalse() {
        // Given
        when(rabbitTemplate.execute(any()))
                .thenThrow(new RuntimeException("Connection failed"));

        // When
        boolean healthy = metricsService.isConnectionHealthy();

        // Then
        assertFalse(healthy);
    }

    @Test
    void testGetHealthStatus_AllHealthy_ReturnsUP() {
        // Given - healthy connection, normal queue depth, adequate consumers
        mockHealthyConnection();
        mockQueueStats(500, 75); // Below 10000 threshold, above 50 minimum

        // When
        RabbitMQMetricsService.HealthStatus status = metricsService.getHealthStatus();

        // Then
        assertEquals(RabbitMQMetricsService.HealthState.UP, status.state());
        assertEquals("All systems operational", status.message());
        assertNotNull(status.details());
    }

    @Test
    void testGetHealthStatus_ConnectionDown_ReturnsDOWN() {
        // Given - connection is not healthy
        when(rabbitTemplate.execute(any()))
                .thenThrow(new RuntimeException("Connection lost"));

        // When
        RabbitMQMetricsService.HealthStatus status = metricsService.getHealthStatus();

        // Then
        assertEquals(RabbitMQMetricsService.HealthState.DOWN, status.state());
        assertEquals("RabbitMQ connection lost", status.message());
    }

    @Test
    void testGetHealthStatus_QueueDepthExceeded_ReturnsDEGRADED() {
        // Given - healthy connection but high queue depth
        mockHealthyConnection();
        mockQueueStats(15000, 75); // Exceeds 10000 threshold

        // When
        RabbitMQMetricsService.HealthStatus status = metricsService.getHealthStatus();

        // Then
        assertEquals(RabbitMQMetricsService.HealthState.DEGRADED, status.state());
        assertEquals("Queue depth exceeds threshold", status.message());
        assertNotNull(status.details());
        assertEquals(15000, status.details().get("queueDepth"));
        assertEquals(10000, status.details().get("threshold"));
    }

    @Test
    void testGetHealthStatus_LowConsumerCount_ReturnsDEGRADED() {
        // Given - healthy connection, normal depth, but low consumers
        mockHealthyConnection();
        mockQueueStats(500, 30); // Below 50 minimum

        // When
        RabbitMQMetricsService.HealthStatus status = metricsService.getHealthStatus();

        // Then
        assertEquals(RabbitMQMetricsService.HealthState.DEGRADED, status.state());
        assertEquals("Consumer count below minimum threshold", status.message());
        assertNotNull(status.details());
        assertEquals(30, status.details().get("actual"));
        assertEquals(50, status.details().get("minimum"));
    }

    @Test
    void testGetHealthStatus_ExceptionDuringCheck_ReturnsDOWN() {
        // Given
        mockHealthyConnection();
        when(rabbitAdmin.getQueueProperties(MAIN_QUEUE))
                .thenThrow(new RuntimeException("Unexpected error"));

        // When
        RabbitMQMetricsService.HealthStatus status = metricsService.getHealthStatus();

        // Then
        assertEquals(RabbitMQMetricsService.HealthState.DOWN, status.state());
        assertTrue(status.message().contains("Health check exception"));
    }

    @Test
    void testGetMetrics_Success() {
        // Given
        mockHealthyConnection();
        mockQueueStats(1000, 60);

        // When
        RabbitMQMetricsService.RabbitMQMetrics metrics = metricsService.getMetrics();

        // Then
        assertNotNull(metrics);
        assertEquals(1000, metrics.queueStats().messageCount());
        assertEquals(60, metrics.queueStats().consumerCount());
        assertEquals(RabbitMQMetricsService.HealthState.UP, metrics.healthState());
        assertTrue(metrics.connectionHealthy());
        assertTrue(metrics.timestamp() > 0);
    }

    @Test
    void testGetQueueDepths_Success() {
        // Given
        mockQueueStats(2500, 70);

        // When
        Map<String, Integer> depths = metricsService.getQueueDepths();

        // Then
        assertNotNull(depths);
        assertEquals(1, depths.size());
        assertTrue(depths.containsKey(MAIN_QUEUE));
        assertEquals(2500, depths.get(MAIN_QUEUE));
    }

    @Test
    void testGetQueueDepths_ExceptionHandled() {
        // Given
        when(rabbitAdmin.getQueueProperties(MAIN_QUEUE))
                .thenThrow(new RuntimeException("Queue not found"));

        // When
        Map<String, Integer> depths = metricsService.getQueueDepths();

        // Then
        assertNotNull(depths);
        assertEquals(1, depths.size());
        assertEquals(-1, depths.get(MAIN_QUEUE)); // Error indicator
    }

    @Test
    void testGetConsumerCounts_Success() {
        // Given
        mockQueueStats(1000, 85);

        // When
        Map<String, Integer> counts = metricsService.getConsumerCounts();

        // Then
        assertNotNull(counts);
        assertEquals(1, counts.size());
        assertTrue(counts.containsKey(MAIN_QUEUE));
        assertEquals(85, counts.get(MAIN_QUEUE));
    }

    @Test
    void testGetConsumerCounts_ExceptionHandled() {
        // Given
        when(rabbitAdmin.getQueueProperties(MAIN_QUEUE))
                .thenThrow(new RuntimeException("Connection timeout"));

        // When
        Map<String, Integer> counts = metricsService.getConsumerCounts();

        // Then
        assertNotNull(counts);
        assertEquals(1, counts.size());
        assertEquals(-1, counts.get(MAIN_QUEUE)); // Error indicator
    }

    @Test
    void testGetMainQueueName() {
        // When
        String queueName = metricsService.getMainQueueName();

        // Then
        assertEquals(MAIN_QUEUE, queueName);
    }

    @Test
    void testGetThresholds() {
        // When
        RabbitMQMetricsService.Thresholds thresholds = metricsService.getThresholds();

        // Then
        assertNotNull(thresholds);
        assertEquals(10000, thresholds.maxQueueDepth());
        assertEquals(50, thresholds.minConsumers());
        assertEquals(100, thresholds.maxConsumers());
    }

    @Test
    void testHealthStatus_BoundaryCondition_ExactThreshold() {
        // Given - queue depth exactly at threshold
        mockHealthyConnection();
        mockQueueStats(10000, 75); // Exactly at 10000 threshold

        // When
        RabbitMQMetricsService.HealthStatus status = metricsService.getHealthStatus();

        // Then - should be UP (not exceeded)
        assertEquals(RabbitMQMetricsService.HealthState.UP, status.state());
    }

    @Test
    void testHealthStatus_BoundaryCondition_OneOverThreshold() {
        // Given - queue depth one over threshold
        mockHealthyConnection();
        mockQueueStats(10001, 75); // One over 10000 threshold

        // When
        RabbitMQMetricsService.HealthStatus status = metricsService.getHealthStatus();

        // Then - should be DEGRADED
        assertEquals(RabbitMQMetricsService.HealthState.DEGRADED, status.state());
    }

    @Test
    void testHealthStatus_BoundaryCondition_MinimumConsumers() {
        // Given - exactly minimum consumers
        mockHealthyConnection();
        mockQueueStats(1000, 50); // Exactly 50 minimum

        // When
        RabbitMQMetricsService.HealthStatus status = metricsService.getHealthStatus();

        // Then - should be UP
        assertEquals(RabbitMQMetricsService.HealthState.UP, status.state());
    }

    @Test
    void testHealthStatus_BoundaryCondition_BelowMinimumConsumers() {
        // Given - one below minimum consumers
        mockHealthyConnection();
        mockQueueStats(1000, 49); // One below 50 minimum

        // When
        RabbitMQMetricsService.HealthStatus status = metricsService.getHealthStatus();

        // Then - should be DEGRADED
        assertEquals(RabbitMQMetricsService.HealthState.DEGRADED, status.state());
    }

    @Test
    void testHealthStatus_ZeroConsumers_ReturnsDEGRADED() {
        // Given
        mockHealthyConnection();
        mockQueueStats(100, 0); // Zero consumers

        // When
        RabbitMQMetricsService.HealthStatus status = metricsService.getHealthStatus();

        // Then
        assertEquals(RabbitMQMetricsService.HealthState.DEGRADED, status.state());
        assertEquals("Consumer count below minimum threshold", status.message());
    }

    @Test
    void testHealthStatus_EmptyQueue_HealthyConsumers_ReturnsUP() {
        // Given
        mockHealthyConnection();
        mockQueueStats(0, 75); // Empty queue but healthy consumers

        // When
        RabbitMQMetricsService.HealthStatus status = metricsService.getHealthStatus();

        // Then
        assertEquals(RabbitMQMetricsService.HealthState.UP, status.state());
    }

    @Test
    void testHealthStatus_HighLoad_MaxConsumers_StillUP() {
        // Given
        mockHealthyConnection();
        mockQueueStats(9999, 100); // High load but at max consumers

        // When
        RabbitMQMetricsService.HealthStatus status = metricsService.getHealthStatus();

        // Then
        assertEquals(RabbitMQMetricsService.HealthState.UP, status.state());
    }

    // Helper methods

    private void mockHealthyConnection() {
        when(rabbitTemplate.execute(any())).thenAnswer(invocation -> {
            org.springframework.amqp.rabbit.connection.ConnectionFactory.ChannelCallback<?> callback =
                    invocation.getArgument(0);
            Channel mockChannel = mock(Channel.class);
            when(mockChannel.isOpen()).thenReturn(true);
            return callback.doInRabbit(mockChannel);
        });
    }

    private void mockQueueStats(int messageCount, int consumerCount) {
        Properties props = new Properties();
        props.put(RabbitAdmin.QUEUE_MESSAGE_COUNT, messageCount);
        props.put(RabbitAdmin.QUEUE_CONSUMER_COUNT, consumerCount);
        when(rabbitAdmin.getQueueProperties(MAIN_QUEUE)).thenReturn(props);
    }
}
