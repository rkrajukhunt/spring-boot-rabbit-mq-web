package com.example.demo.service;

import com.example.demo.exception.MessageProcessingException;
import com.example.demo.model.dto.MessagePayload;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class CommunicationService {

    private final ObjectMapper objectMapper;

    /**
     * This is the heavy business logic method that was causing load issues.
     * Replace this implementation with your actual communication creation logic.
     */
    public void createCommunication(MessagePayload payload) {
        log.info("Starting createCommunication for tracking ID: {}", payload.getTrackingId());

        try {
            // Parse payload
            Map<String, Object> data = parsePayload(payload.getPayload());

            // YOUR ACTUAL BUSINESS LOGIC HERE
            // Examples:
            // - Send emails/notifications
            // - Call external APIs
            // - Database operations
            // - File processing
            // - Complex calculations

            // Simulate heavy processing (remove this in production)
            processBusinessLogic(data, payload);

            log.info("Completed createCommunication for tracking ID: {}", payload.getTrackingId());

        } catch (Exception e) {
            log.error("Error in createCommunication for tracking ID: {}", payload.getTrackingId(), e);
            throw new MessageProcessingException("Communication processing failed", e);
        }
    }

    /**
     * Parse JSON payload into structured data
     */
    private Map<String, Object> parsePayload(String payload) {
        try {
            return objectMapper.readValue(payload, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception e) {
            throw new MessageProcessingException("Invalid payload format", e);
        }
    }

    /**
     * Simulate business logic processing
     * Replace this with your actual implementation
     */
    private void processBusinessLogic(Map<String, Object> data, MessagePayload payload) {
        try {
            // Simulate processing time (remove in production)
            Thread.sleep(2000); // 2 seconds

            log.debug("Processing data: {} for tracking ID: {}", data, payload.getTrackingId());

            // Add your actual business logic here:
            // 1. Validate data
            // 2. Transform data
            // 3. Call external services
            // 4. Persist to database
            // 5. Send notifications
            // etc.

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessageProcessingException("Processing interrupted", e);
        }
    }
}
