package com.example.demo.enums;

public enum MessageStatus {
    RECEIVED,       // Initial status when message is received via API
    PROCESSING,     // Consumer is actively processing the message
    COMPLETED,      // Message processed successfully
    FAILED,         // Message processing failed (general failure)
    RETRY,          // Message sent to retry queue
    DEAD_LETTER     // Message moved to DLQ after all retries exhausted
}
