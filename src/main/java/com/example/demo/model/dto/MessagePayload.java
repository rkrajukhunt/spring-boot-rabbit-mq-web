package com.example.demo.model.dto;

import com.example.demo.enums.MessagePriority;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessagePayload implements Serializable {

    private static final long serialVersionUID = 1L;

    private String trackingId;
    private String payload;
    private MessagePriority priority;
    private Integer retryCount;
    private LocalDateTime timestamp;
    private Map<String, Object> metadata;

    // Tracking information for traceability
    private String createRequestId;  // Original create request ID for correlation
    private Long processingStartTime; // Timestamp when processing started (for metrics)
    private Long processingEndTime;   // Timestamp when processing completed (for metrics)
}
