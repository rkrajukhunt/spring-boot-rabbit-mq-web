package com.example.demo.consumer;

import com.example.demo.enums.MessagePriority;
import com.example.demo.enums.MessageStatus;
import com.example.demo.model.dto.MessagePayload;
import com.example.demo.service.CommunicationService;
import com.example.demo.service.TrackingService;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageConsumerTest {

    @Mock
    private TrackingService trackingService;

    @Mock
    private CommunicationService communicationService;

    @Mock
    private RabbitTemplate rabbitTemplate;

    @Mock
    private Channel channel;

    @InjectMocks
    private MessageConsumer messageConsumer;

    @Captor
    private ArgumentCaptor<MessagePayload> payloadCaptor;

    private static final String QUEUE_NAME = "inappcommunication.messages-fed";
    private static final int MAX_RETRY_ATTEMPTS = 3;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(messageConsumer, "maxRetryAttempts", MAX_RETRY_ATTEMPTS);
    }

    @Test
    void testConsumePriorityQueue_Success() throws IOException {
        // Given
        String trackingId = "track-123";
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", "user-1");

        MessagePayload messagePayload = MessagePayload.builder()
                .trackingId(trackingId)
                .payload(payload)
                .priority(MessagePriority.HIGH)
                .retryCount(0)
                .timestamp(LocalDateTime.now())
                .build();

        long deliveryTag = 1L;
        Integer messagePriority = 10;

        // When
        messageConsumer.consumePriorityQueue(messagePayload, deliveryTag, messagePriority, channel);

        // Then
        verify(trackingService).updateStatus(trackingId, MessageStatus.PROCESSING, null);
        verify(communicationService).createCommunication(messagePayload);
        verify(trackingService).updateStatus(trackingId, MessageStatus.COMPLETED, null);
        verify(trackingService).markCompleted(trackingId);
        verify(channel).basicAck(deliveryTag, false);
        verifyNoMoreInteractions(rabbitTemplate);
    }

    @Test
    void testConsumePriorityQueue_HighPriority_Success() throws IOException {
        // Given
        MessagePayload messagePayload = createMessagePayload("track-high", MessagePriority.HIGH, 0);
        long deliveryTag = 2L;
        Integer messagePriority = 10; // HIGH priority value

        // When
        messageConsumer.consumePriorityQueue(messagePayload, deliveryTag, messagePriority, channel);

        // Then
        verify(channel).basicAck(deliveryTag, false);
        verify(trackingService).updateStatus("track-high", MessageStatus.COMPLETED, null);
    }

    @Test
    void testConsumePriorityQueue_MediumPriority_Success() throws IOException {
        // Given
        MessagePayload messagePayload = createMessagePayload("track-medium", MessagePriority.MEDIUM, 0);
        long deliveryTag = 3L;
        Integer messagePriority = 5; // MEDIUM priority value

        // When
        messageConsumer.consumePriorityQueue(messagePayload, deliveryTag, messagePriority, channel);

        // Then
        verify(channel).basicAck(deliveryTag, false);
        verify(trackingService).updateStatus("track-medium", MessageStatus.COMPLETED, null);
    }

    @Test
    void testConsumePriorityQueue_LowPriority_Success() throws IOException {
        // Given
        MessagePayload messagePayload = createMessagePayload("track-low", MessagePriority.LOW, 0);
        long deliveryTag = 4L;
        Integer messagePriority = 1; // LOW priority value

        // When
        messageConsumer.consumePriorityQueue(messagePayload, deliveryTag, messagePriority, channel);

        // Then
        verify(channel).basicAck(deliveryTag, false);
        verify(trackingService).updateStatus("track-low", MessageStatus.COMPLETED, null);
    }

    @Test
    void testConsumePriorityQueue_NullPriority_Success() throws IOException {
        // Given - message without priority header
        MessagePayload messagePayload = createMessagePayload("track-no-priority", MessagePriority.MEDIUM, 0);
        long deliveryTag = 5L;
        Integer messagePriority = null; // No priority header

        // When
        messageConsumer.consumePriorityQueue(messagePayload, deliveryTag, messagePriority, channel);

        // Then
        verify(channel).basicAck(deliveryTag, false);
        verify(trackingService).updateStatus("track-no-priority", MessageStatus.COMPLETED, null);
    }

    @Test
    void testConsumePriorityQueue_FirstRetry() throws IOException {
        // Given
        MessagePayload messagePayload = createMessagePayload("track-retry-1", MessagePriority.HIGH, 0);
        long deliveryTag = 6L;

        doThrow(new RuntimeException("Processing failed"))
                .when(communicationService).createCommunication(any());

        // When
        messageConsumer.consumePriorityQueue(messagePayload, deliveryTag, 10, channel);

        // Then
        verify(trackingService).updateStatus("track-retry-1", MessageStatus.PROCESSING, null);
        verify(rabbitTemplate).convertAndSend(
                eq("inappcommunication.messages-retry-1-fed"),
                payloadCaptor.capture(),
                any()
        );

        MessagePayload sentPayload = payloadCaptor.getValue();
        assertEquals(1, sentPayload.getRetryCount());
        assertEquals(MessagePriority.HIGH, sentPayload.getPriority());

        verify(channel).basicAck(deliveryTag, false);
        verify(trackingService).updateStatus(eq("track-retry-1"), eq(MessageStatus.RETRY), anyString());
        verify(trackingService).incrementRetryCount("track-retry-1");
    }

    @Test
    void testConsumePriorityQueue_SecondRetry() throws IOException {
        // Given
        MessagePayload messagePayload = createMessagePayload("track-retry-2", MessagePriority.MEDIUM, 1);
        long deliveryTag = 7L;

        doThrow(new RuntimeException("Processing failed again"))
                .when(communicationService).createCommunication(any());

        // When
        messageConsumer.consumePriorityQueue(messagePayload, deliveryTag, 5, channel);

        // Then
        verify(rabbitTemplate).convertAndSend(
                eq("inappcommunication.messages-retry-2-fed"),
                payloadCaptor.capture(),
                any()
        );

        MessagePayload sentPayload = payloadCaptor.getValue();
        assertEquals(2, sentPayload.getRetryCount());
        verify(channel).basicAck(deliveryTag, false);
    }

    @Test
    void testConsumePriorityQueue_ThirdRetry() throws IOException {
        // Given
        MessagePayload messagePayload = createMessagePayload("track-retry-3", MessagePriority.LOW, 2);
        long deliveryTag = 8L;

        doThrow(new RuntimeException("Processing failed third time"))
                .when(communicationService).createCommunication(any());

        // When
        messageConsumer.consumePriorityQueue(messagePayload, deliveryTag, 1, channel);

        // Then
        verify(rabbitTemplate).convertAndSend(
                eq("inappcommunication.messages-retry-3-fed"),
                payloadCaptor.capture(),
                any()
        );

        MessagePayload sentPayload = payloadCaptor.getValue();
        assertEquals(3, sentPayload.getRetryCount());
        verify(channel).basicAck(deliveryTag, false);
    }

    @Test
    void testConsumePriorityQueue_MaxRetriesExhausted_SendToDLQ() throws IOException {
        // Given
        MessagePayload messagePayload = createMessagePayload("track-dlq", MessagePriority.HIGH, MAX_RETRY_ATTEMPTS);
        long deliveryTag = 9L;

        doThrow(new RuntimeException("Final processing failure"))
                .when(communicationService).createCommunication(any());

        // When
        messageConsumer.consumePriorityQueue(messagePayload, deliveryTag, 10, channel);

        // Then
        verify(rabbitTemplate).convertAndSend(
                eq("inappcommunication.messages-dlq-fed"),
                payloadCaptor.capture(),
                any()
        );

        MessagePayload sentPayload = payloadCaptor.getValue();
        assertEquals(MAX_RETRY_ATTEMPTS, sentPayload.getRetryCount());
        assertEquals(MessagePriority.HIGH, sentPayload.getPriority());

        verify(channel).basicAck(deliveryTag, false);
        verify(trackingService).updateStatus(
                eq("track-dlq"),
                eq(MessageStatus.DEAD_LETTER),
                contains("Max retries exhausted")
        );
        verify(trackingService, never()).incrementRetryCount(anyString());
    }

    @Test
    void testConsumePriorityQueue_PreservesPriorityDuringRetry() throws IOException {
        // Given - HIGH priority message that fails
        MessagePayload highPriorityPayload = createMessagePayload("track-priority-preserve", MessagePriority.HIGH, 0);
        long deliveryTag = 10L;

        doThrow(new RuntimeException("Processing failed"))
                .when(communicationService).createCommunication(any());

        // When
        messageConsumer.consumePriorityQueue(highPriorityPayload, deliveryTag, 10, channel);

        // Then - verify priority is preserved in retry
        verify(rabbitTemplate).convertAndSend(
                anyString(),
                payloadCaptor.capture(),
                any()
        );

        MessagePayload retryPayload = payloadCaptor.getValue();
        assertEquals(MessagePriority.HIGH, retryPayload.getPriority());
        assertEquals(10, MessagePriority.HIGH.getPriorityValue());
    }

    @Test
    void testConsumePriorityQueue_PreservesPriorityInDLQ() throws IOException {
        // Given - Message at max retries
        MessagePayload messagePayload = createMessagePayload("track-dlq-priority", MessagePriority.MEDIUM, MAX_RETRY_ATTEMPTS);
        long deliveryTag = 11L;

        doThrow(new RuntimeException("Final failure"))
                .when(communicationService).createCommunication(any());

        // When
        messageConsumer.consumePriorityQueue(messagePayload, deliveryTag, 5, channel);

        // Then - verify priority is preserved in DLQ
        verify(rabbitTemplate).convertAndSend(
                eq("inappcommunication.messages-dlq-fed"),
                payloadCaptor.capture(),
                any()
        );

        MessagePayload dlqPayload = payloadCaptor.getValue();
        assertEquals(MessagePriority.MEDIUM, dlqPayload.getPriority());
        assertEquals(5, MessagePriority.MEDIUM.getPriorityValue());
    }

    @Test
    void testConsumePriorityQueue_ErrorMessageIncludesException() throws IOException {
        // Given
        MessagePayload messagePayload = createMessagePayload("track-error-msg", MessagePriority.HIGH, MAX_RETRY_ATTEMPTS);
        long deliveryTag = 12L;
        String errorMessage = "Database connection timeout";

        doThrow(new RuntimeException(errorMessage))
                .when(communicationService).createCommunication(any());

        // When
        messageConsumer.consumePriorityQueue(messagePayload, deliveryTag, 10, channel);

        // Then
        verify(trackingService).updateStatus(
                eq("track-error-msg"),
                eq(MessageStatus.DEAD_LETTER),
                contains(errorMessage)
        );
    }

    @Test
    void testHandleDLQ_LogsFailedMessage() {
        // Given
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", "user-dlq");

        MessagePayload dlqPayload = MessagePayload.builder()
                .trackingId("track-dlq-listener")
                .payload(payload)
                .priority(MessagePriority.HIGH)
                .retryCount(MAX_RETRY_ATTEMPTS)
                .timestamp(LocalDateTime.now())
                .build();

        Integer messagePriority = 10;
        org.springframework.amqp.core.Message message = mock(org.springframework.amqp.core.Message.class);

        // When - should not throw exception
        assertDoesNotThrow(() ->
                messageConsumer.handleDLQ(dlqPayload, messagePriority, message)
        );

        // Then - verify no interactions (just logging)
        verifyNoInteractions(trackingService, communicationService, rabbitTemplate);
    }

    @Test
    void testConsumePriorityQueue_MultipleMessagesInSequence() throws IOException {
        // Given - simulate processing multiple messages
        String[] trackingIds = {"track-seq-1", "track-seq-2", "track-seq-3"};
        MessagePriority[] priorities = {MessagePriority.HIGH, MessagePriority.MEDIUM, MessagePriority.LOW};

        // When
        for (int i = 0; i < trackingIds.length; i++) {
            MessagePayload messagePayload = createMessagePayload(trackingIds[i], priorities[i], 0);
            messageConsumer.consumePriorityQueue(messagePayload, i + 1, priorities[i].getPriorityValue(), channel);
        }

        // Then
        verify(channel, times(3)).basicAck(anyLong(), eq(false));
        verify(trackingService, times(3)).updateStatus(anyString(), eq(MessageStatus.COMPLETED), isNull());
        verify(trackingService, times(3)).markCompleted(anyString());
    }

    @Test
    void testConsumePriorityQueue_WithMetadata() throws IOException {
        // Given
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", "user-meta");

        Map<String, String> metadata = new HashMap<>();
        metadata.put("source", "api");
        metadata.put("version", "1.0");

        MessagePayload messagePayload = MessagePayload.builder()
                .trackingId("track-metadata")
                .payload(payload)
                .priority(MessagePriority.HIGH)
                .retryCount(0)
                .timestamp(LocalDateTime.now())
                .metadata(metadata)
                .build();

        long deliveryTag = 100L;

        // When
        messageConsumer.consumePriorityQueue(messagePayload, deliveryTag, 10, channel);

        // Then
        verify(communicationService).createCommunication(argThat(p ->
                p.getMetadata() != null &&
                        p.getMetadata().get("source").equals("api") &&
                        p.getMetadata().get("version").equals("1.0")
        ));
        verify(channel).basicAck(deliveryTag, false);
    }

    // Helper method to create test message payload
    private MessagePayload createMessagePayload(String trackingId, MessagePriority priority, int retryCount) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", "user-" + trackingId);
        payload.put("message", "Test message");

        return MessagePayload.builder()
                .trackingId(trackingId)
                .payload(payload)
                .priority(priority)
                .retryCount(retryCount)
                .timestamp(LocalDateTime.now())
                .build();
    }
}
