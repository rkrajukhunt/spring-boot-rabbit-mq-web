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
     * Mark message as completed
     */
    public void markCompleted(String trackingId) {
        repository.findByTrackingId(trackingId).ifPresent(tracking -> {
            tracking.setCompletedAt(LocalDateTime.now());
            repository.save(tracking);
            log.info("Marked tracking ID {} as completed", trackingId);
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
     * Get statistics
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("total", repository.count());
        stats.put("received", repository.countByStatus(MessageStatus.RECEIVED));
        stats.put("processing", repository.countByStatus(MessageStatus.PROCESSING));
        stats.put("completed", repository.countByStatus(MessageStatus.COMPLETED));
        stats.put("failed", repository.countByStatus(MessageStatus.FAILED));
        stats.put("retry", repository.countByStatus(MessageStatus.RETRY));
        stats.put("dead_letter", repository.countByStatus(MessageStatus.DEAD_LETTER));
        log.debug("Generated statistics: {}", stats);
        return stats;
    }
}
