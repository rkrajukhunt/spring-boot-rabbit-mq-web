package com.example.demo.service;

import com.example.demo.enums.MessagePriority;
import com.example.demo.enums.MessageStatus;
import com.example.demo.exception.MessageProcessingException;
import com.example.demo.model.dto.MessagePayload;
import com.example.demo.model.dto.MessageRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessagePublisherServiceTest {

    @Mock
    private RabbitTemplate rabbitTemplate;

    @Mock
    private TrackingService trackingService;

    @InjectMocks
    private MessagePublisherService messagePublisherService;

    @Captor
    private ArgumentCaptor<String> exchangeCaptor;

    @Captor
    private ArgumentCaptor<String> routingKeyCaptor;

    @Captor
    private ArgumentCaptor<MessagePayload> payloadCaptor;

    @Captor
    private ArgumentCaptor<CorrelationData> correlationDataCaptor;

    private static final String EXCHANGE_NAME = "inappcommunication.communication-fed";
    private static final String ROUTING_KEY = "messages.priority";
    private static final String QUEUE_NAME = "inappcommunication.messages-fed";

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(messagePublisherService, "exchangeName", EXCHANGE_NAME);
    }

    @Test
    void testPublishMessage_WithHighPriority_Success() {
        // Given
        String trackingId = "track-123";
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", "user-1");
        payload.put("message", "Test message");

        Map<String, String> metadata = new HashMap<>();
        metadata.put("source", "unit-test");

        MessageRequest request = MessageRequest.builder()
                .payload(payload)
                .priority(MessagePriority.HIGH)
                .metadata(metadata)
                .build();

        // When
        messagePublisherService.publishMessage(trackingId, request);

        // Then
        verify(rabbitTemplate).convertAndSend(
                exchangeCaptor.capture(),
                routingKeyCaptor.capture(),
                payloadCaptor.capture(),
                any(),
                correlationDataCaptor.capture()
        );

        assertEquals(EXCHANGE_NAME, exchangeCaptor.getValue());
        assertEquals(ROUTING_KEY, routingKeyCaptor.getValue());

        MessagePayload capturedPayload = payloadCaptor.getValue();
        assertEquals(trackingId, capturedPayload.getTrackingId());
        assertEquals(MessagePriority.HIGH, capturedPayload.getPriority());
        assertEquals(0, capturedPayload.getRetryCount());
        assertEquals(payload, capturedPayload.getPayload());
        assertEquals(metadata, capturedPayload.getMetadata());

        assertEquals(trackingId, correlationDataCaptor.getValue().getId());

        verify(trackingService).updateQueueName(trackingId, QUEUE_NAME);
        verifyNoMoreInteractions(trackingService);
    }

    @Test
    void testPublishMessage_WithMediumPriority_Success() {
        // Given
        String trackingId = "track-456";
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", "user-2");

        MessageRequest request = MessageRequest.builder()
                .payload(payload)
                .priority(MessagePriority.MEDIUM)
                .build();

        // When
        messagePublisherService.publishMessage(trackingId, request);

        // Then
        verify(rabbitTemplate).convertAndSend(
                eq(EXCHANGE_NAME),
                eq(ROUTING_KEY),
                payloadCaptor.capture(),
                any(),
                any(CorrelationData.class)
        );

        MessagePayload capturedPayload = payloadCaptor.getValue();
        assertEquals(MessagePriority.MEDIUM, capturedPayload.getPriority());
        verify(trackingService).updateQueueName(trackingId, QUEUE_NAME);
    }

    @Test
    void testPublishMessage_WithLowPriority_Success() {
        // Given
        String trackingId = "track-789";
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", "user-3");

        MessageRequest request = MessageRequest.builder()
                .payload(payload)
                .priority(MessagePriority.LOW)
                .build();

        // When
        messagePublisherService.publishMessage(trackingId, request);

        // Then
        verify(rabbitTemplate).convertAndSend(
                eq(EXCHANGE_NAME),
                eq(ROUTING_KEY),
                payloadCaptor.capture(),
                any(),
                any(CorrelationData.class)
        );

        MessagePayload capturedPayload = payloadCaptor.getValue();
        assertEquals(MessagePriority.LOW, capturedPayload.getPriority());
        verify(trackingService).updateQueueName(trackingId, QUEUE_NAME);
    }

    @Test
    void testPublishMessage_NoPriorityProvided_DefaultsToMedium() {
        // Given
        String trackingId = "track-default";
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", "user-4");

        MessageRequest request = MessageRequest.builder()
                .payload(payload)
                .priority(null) // No priority specified
                .build();

        // When
        messagePublisherService.publishMessage(trackingId, request);

        // Then
        verify(rabbitTemplate).convertAndSend(
                eq(EXCHANGE_NAME),
                eq(ROUTING_KEY),
                payloadCaptor.capture(),
                any(),
                any(CorrelationData.class)
        );

        MessagePayload capturedPayload = payloadCaptor.getValue();
        assertEquals(MessagePriority.MEDIUM, capturedPayload.getPriority());
        verify(trackingService).updateQueueName(trackingId, QUEUE_NAME);
    }

    @Test
    void testPublishMessage_WithMetadata_Success() {
        // Given
        String trackingId = "track-meta";
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", "user-5");

        Map<String, String> metadata = new HashMap<>();
        metadata.put("source", "api");
        metadata.put("version", "1.0");
        metadata.put("clientId", "client-123");

        MessageRequest request = MessageRequest.builder()
                .payload(payload)
                .priority(MessagePriority.HIGH)
                .metadata(metadata)
                .build();

        // When
        messagePublisherService.publishMessage(trackingId, request);

        // Then
        verify(rabbitTemplate).convertAndSend(
                eq(EXCHANGE_NAME),
                eq(ROUTING_KEY),
                payloadCaptor.capture(),
                any(),
                any(CorrelationData.class)
        );

        MessagePayload capturedPayload = payloadCaptor.getValue();
        assertEquals(metadata, capturedPayload.getMetadata());
        assertEquals(3, capturedPayload.getMetadata().size());
    }

    @Test
    void testPublishMessage_AmqpException_UpdatesStatusAndThrows() {
        // Given
        String trackingId = "track-error";
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", "user-6");

        MessageRequest request = MessageRequest.builder()
                .payload(payload)
                .priority(MessagePriority.HIGH)
                .build();

        AmqpException amqpException = new AmqpException("Connection failed");
        doThrow(amqpException).when(rabbitTemplate).convertAndSend(
                anyString(),
                anyString(),
                any(MessagePayload.class),
                any(),
                any(CorrelationData.class)
        );

        // When & Then
        MessageProcessingException exception = assertThrows(
                MessageProcessingException.class,
                () -> messagePublisherService.publishMessage(trackingId, request)
        );

        assertEquals("Failed to publish message to RabbitMQ", exception.getMessage());
        assertEquals(amqpException, exception.getCause());

        verify(trackingService).updateStatus(trackingId, MessageStatus.FAILED, "Connection failed");
        verify(trackingService, never()).updateQueueName(anyString(), anyString());
    }

    @Test
    void testPublishMessage_VerifyMessageProperties() {
        // Given
        String trackingId = "track-props";
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", "user-7");

        MessageRequest request = MessageRequest.builder()
                .payload(payload)
                .priority(MessagePriority.HIGH)
                .build();

        // Mock the message postprocessor to verify properties
        doAnswer(invocation -> {
            org.springframework.amqp.core.MessagePostProcessor postProcessor =
                invocation.getArgument(3);

            // Create a mock message to test the postprocessor
            Message message = mock(Message.class);
            org.springframework.amqp.core.MessageProperties props =
                new org.springframework.amqp.core.MessageProperties();
            when(message.getMessageProperties()).thenReturn(props);

            // Apply the postprocessor
            Message processedMessage = postProcessor.postProcessMessage(message);

            // Verify the properties set by postprocessor
            assertEquals(trackingId, processedMessage.getMessageProperties().getCorrelationId());
            assertEquals(MessagePriority.HIGH.getPriorityValue(),
                        processedMessage.getMessageProperties().getPriority());
            assertEquals(MessageDeliveryMode.PERSISTENT,
                        processedMessage.getMessageProperties().getDeliveryMode());

            return null;
        }).when(rabbitTemplate).convertAndSend(
                anyString(),
                anyString(),
                any(MessagePayload.class),
                any(),
                any(CorrelationData.class)
        );

        // When
        messagePublisherService.publishMessage(trackingId, request);

        // Then - verification happens in the doAnswer lambda above
        verify(rabbitTemplate).convertAndSend(
                anyString(),
                anyString(),
                any(MessagePayload.class),
                any(),
                any(CorrelationData.class)
        );
    }

    @Test
    void testPublishMessage_VerifyRetryCountIsZero() {
        // Given
        String trackingId = "track-retry";
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", "user-8");

        MessageRequest request = MessageRequest.builder()
                .payload(payload)
                .priority(MessagePriority.MEDIUM)
                .build();

        // When
        messagePublisherService.publishMessage(trackingId, request);

        // Then
        verify(rabbitTemplate).convertAndSend(
                eq(EXCHANGE_NAME),
                eq(ROUTING_KEY),
                payloadCaptor.capture(),
                any(),
                any(CorrelationData.class)
        );

        MessagePayload capturedPayload = payloadCaptor.getValue();
        assertEquals(0, capturedPayload.getRetryCount());
        assertNotNull(capturedPayload.getTimestamp());
    }

    @Test
    void testPublishMessage_MultipleMessages_Success() {
        // Given
        String[] trackingIds = {"track-1", "track-2", "track-3"};
        MessagePriority[] priorities = {MessagePriority.HIGH, MessagePriority.MEDIUM, MessagePriority.LOW};

        // When
        for (int i = 0; i < trackingIds.length; i++) {
            Map<String, Object> payload = new HashMap<>();
            payload.put("userId", "user-" + i);

            MessageRequest request = MessageRequest.builder()
                    .payload(payload)
                    .priority(priorities[i])
                    .build();

            messagePublisherService.publishMessage(trackingIds[i], request);
        }

        // Then
        verify(rabbitTemplate, times(3)).convertAndSend(
                eq(EXCHANGE_NAME),
                eq(ROUTING_KEY),
                any(MessagePayload.class),
                any(),
                any(CorrelationData.class)
        );

        verify(trackingService, times(3)).updateQueueName(anyString(), eq(QUEUE_NAME));
    }
}