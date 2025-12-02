package com.example.demo.controller;

import com.example.demo.enums.MessageStatus;
import com.example.demo.model.dto.MessageRequest;
import com.example.demo.model.dto.MessageResponse;
import com.example.demo.model.entity.MessageTracking;
import com.example.demo.service.MessagePublisherService;
import com.example.demo.service.TrackingService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/messages")
@Slf4j
@RequiredArgsConstructor
public class MessageController {

    private final MessagePublisherService publisherService;
    private final TrackingService trackingService;

    /**
     * Publish message to RabbitMQ
     * Returns tracking ID immediately for async processing
     */
    @PostMapping("/publish")
    public ResponseEntity<MessageResponse> publishMessage(@Valid @RequestBody MessageRequest request) {
        log.info("Received message request with priority: {}", request.getPriority());

        // Generate unique tracking ID
        String trackingId = trackingService.generateTrackingId();

        // Save initial tracking record (RECEIVED status)
        trackingService.createTracking(trackingId, request);

        // Publish to RabbitMQ (non-blocking)
        publisherService.publishMessage(trackingId, request);

        // Return immediate response with tracking ID
        MessageResponse response = MessageResponse.builder()
                .trackingId(trackingId)
                .status(MessageStatus.RECEIVED)
                .message("Message accepted for processing")
                .timestamp(LocalDateTime.now())
                .build();

        log.info("Message published successfully with tracking ID: {}", trackingId);

        return ResponseEntity.accepted().body(response);
    }

    /**
     * Get message status by tracking ID
     */
    @GetMapping("/track/{trackingId}")
    public ResponseEntity<MessageTracking> trackMessage(@PathVariable String trackingId) {
        log.debug("Tracking request for ID: {}", trackingId);
        MessageTracking tracking = trackingService.getTracking(trackingId);
        return ResponseEntity.ok(tracking);
    }

    /**
     * Get system statistics
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        log.debug("Statistics request");
        Map<String, Object> stats = trackingService.getStatistics();
        return ResponseEntity.ok(stats);
    }
}
