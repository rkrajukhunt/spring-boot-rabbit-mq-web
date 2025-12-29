package com.example.demo.service;

import com.example.demo.enums.MessageStatus;
import com.example.demo.exception.MessageProcessingException;
import com.example.demo.exception.ResourceNotFoundException;
import com.example.demo.model.dto.MessageRequest;
import com.example.demo.model.entity.MessageTracking;
import com.example.demo.repository.MessageTrackingRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class TrackingService {

    private final MessageTrackingRepository repository;
    private final ObjectMapper objectMapper;

    @Value("${app.tracking.id-prefix}")
    private String idPrefix;

    /**
     * Generate unique tracking ID
     */
    public String generateTrackingId() {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String trackingId = idPrefix + "-" + uuid.substring(0, 20).toUpperCase();
        log.debug("Generated tracking ID: {}", trackingId);
        return trackingId;
    }

    /**
     * Create initial tracking record with RECEIVED status
     */
    public MessageTracking createTracking(String trackingId, MessageRequest request) {
        try {
            MessageTracking tracking = new MessageTracking();
            tracking.setTrackingId(trackingId);
            tracking.setPayload(objectMapper.writeValueAsString(request.getPayload()));
            tracking.setPriority(request.getPriority());
            tracking.setStatus(MessageStatus.RECEIVED);
            tracking.setRetryCount(0);

            MessageTracking saved = repository.save(tracking);
            log.info("Created tracking record for ID: {} with priority: {}", trackingId, request.getPriority());
            return saved;

        } catch (JsonProcessingException e) {
            log.error("Failed to serialize payload for tracking ID: {}", trackingId, e);
            throw new MessageProcessingException("Failed to serialize payload", e);
        }
    }

    /**
     * Update message status
     */
    public void updateStatus(String trackingId, MessageStatus status, String errorMessage) {
        repository.findByTrackingId(trackingId).ifPresent(tracking -> {
            tracking.setStatus(status);
            if (errorMessage != null) {
                tracking.setErrorMessage(errorMessage);
            }
            repository.save(tracking);
            log.debug("Updated tracking ID {} to status: {}", trackingId, status);
        });
    }

    /**
     * Update queue name
     */
    public void updateQueueName(String trackingId, String queueName) {
        repository.findByTrackingId(trackingId).ifPresent(tracking -> {
            tracking.setQueueName(queueName);
            repository.save(tracking);
            log.debug("Updated tracking ID {} with queue name: {}", trackingId, queueName);
        });
    }

    /**
     * Increment retry count
     */
    public void incrementRetryCount(String trackingId) {
        repository.findByTrackingId(trackingId).ifPresent(tracking -> {
            tracking.setRetryCount(tracking.getRetryCount() + 1);
            repository.save(tracking);
            log.debug("Incremented retry count for tracking ID {} to: {}", trackingId, tracking.getRetryCount());
        });
    }

    /**
     * Mark processing started - records the start time for duration calculation
     */
    public void markProcessingStarted(String trackingId) {
        repository.findByTrackingId(trackingId).ifPresent(tracking -> {
            tracking.setProcessingStartedAt(LocalDateTime.now());
            repository.save(tracking);
            log.debug("Marked processing started for tracking ID {}", trackingId);
        });
    }

    /**
     * Mark message as completed with processing duration
     */
    public void markCompleted(String trackingId) {
        repository.findByTrackingId(trackingId).ifPresent(tracking -> {
            LocalDateTime now = LocalDateTime.now();
            tracking.setCompletedAt(now);

            // Calculate processing duration if start time was recorded
            if (tracking.getProcessingStartedAt() != null) {
                long durationMs = java.time.Duration.between(
                        tracking.getProcessingStartedAt(), now).toMillis();
                tracking.setProcessingDurationMs(durationMs);
                log.info("Marked tracking ID {} as completed (duration: {}ms)", trackingId, durationMs);
            } else {
                log.info("Marked tracking ID {} as completed", trackingId);
            }

            repository.save(tracking);
        });
    }

    /**
     * Mark message as completed with explicit duration (from payload timestamps)
     */
    public void markCompletedWithDuration(String trackingId, long processingDurationMs) {
        repository.findByTrackingId(trackingId).ifPresent(tracking -> {
            tracking.setCompletedAt(LocalDateTime.now());
            tracking.setProcessingDurationMs(processingDurationMs);
            repository.save(tracking);
            log.info("Marked tracking ID {} as completed (duration: {}ms)", trackingId, processingDurationMs);
        });
    }

    /**
     * Get tracking record by ID
     */
    public MessageTracking getTracking(String trackingId) {
        return repository.findByTrackingId(trackingId)
                .orElseThrow(() -> new ResourceNotFoundException("Tracking ID not found: " + trackingId));
    }

    /**
     * Get statistics including processing duration metrics
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();

        // Message counts by status
        stats.put("total", repository.count());
        stats.put("received", repository.countByStatus(MessageStatus.RECEIVED));
        stats.put("processing", repository.countByStatus(MessageStatus.PROCESSING));
        stats.put("completed", repository.countByStatus(MessageStatus.COMPLETED));
        stats.put("failed", repository.countByStatus(MessageStatus.FAILED));
        stats.put("retry", repository.countByStatus(MessageStatus.RETRY));
        stats.put("dead_letter", repository.countByStatus(MessageStatus.DEAD_LETTER));

        // Processing duration metrics
        stats.put("processed_count", repository.countProcessedMessages());
        Double avgDuration = repository.getAverageProcessingDuration();
        stats.put("avg_processing_duration_ms", avgDuration != null ? avgDuration : 0.0);

        // Last hour average
        Double avgDurationLastHour = repository.getAverageProcessingDurationSince(
                LocalDateTime.now().minusHours(1));
        stats.put("avg_processing_duration_ms_last_hour", avgDurationLastHour != null ? avgDurationLastHour : 0.0);

        log.debug("Generated statistics: {}", stats);
        return stats;
    }
}
