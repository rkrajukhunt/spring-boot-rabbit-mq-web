package com.example.demo.enums;

import lombok.Getter;

import java.util.List;

@Getter
public enum MessagePriority {
    HIGH("message.priority.high", 10, List.of("message-queue-high-1", "message-queue-high-2")),
    MEDIUM("message.priority.medium", 5, List.of("message-queue-medium-1", "message-queue-medium-2")),
    LOW("message.priority.low", 1, List.of("message-queue-low-1", "message-queue-low-2"));

    private final String routingKey;
    private final int priorityValue;
    private final List<String> queueNames;

    MessagePriority(String routingKey, int priorityValue, List<String> queueNames) {
        this.routingKey = routingKey;
        this.priorityValue = priorityValue;
        this.queueNames = queueNames;
    }

    /**
     * Get all queue names for this priority level
     */
    public List<String> getAllQueues() {
        return queueNames;
    }

    /**
     * Get specific queue by index (0 or 1)
     */
    public String getQueueByIndex(int index) {
        return queueNames.get(index % queueNames.size());
    }
}
