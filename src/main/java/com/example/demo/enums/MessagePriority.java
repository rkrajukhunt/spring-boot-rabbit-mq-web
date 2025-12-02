package com.example.demo.enums;

import lombok.Getter;

@Getter
public enum MessagePriority {
    HIGH("message.priority.high", "message-queue-1", 10),
    MEDIUM("message.priority.medium", "message-queue-2", 5),
    LOW("message.priority.low", "message-queue-3", 1);

    private final String routingKey;
    private final String queueName;
    private final int priorityValue;

    MessagePriority(String routingKey, String queueName, int priorityValue) {
        this.routingKey = routingKey;
        this.queueName = queueName;
        this.priorityValue = priorityValue;
    }
}
