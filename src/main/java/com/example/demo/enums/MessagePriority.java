package com.example.demo.enums;

import lombok.Getter;

/**
 * Message priority levels using RabbitMQ native priority queue support.
 * Priority values range from 0-10, where 10 is the highest priority.
 *
 * RabbitMQ processes higher priority messages first when multiple messages are queued.
 * See: https://www.rabbitmq.com/docs/priority
 */
@Getter
public enum MessagePriority {
    /**
     * High priority messages (priority=10)
     * Processed first when queue has backlog
     */
    HIGH(10),

    /**
     * Medium priority messages (priority=5)
     * Default priority for messages without explicit priority
     */
    MEDIUM(5),

    /**
     * Low priority messages (priority=1)
     * Processed last, suitable for background tasks
     */
    LOW(1);

    /**
     * RabbitMQ priority value (0-10, where 10 is highest)
     * This value is set in the message properties and used by RabbitMQ for ordering
     */
    private final int priorityValue;

    MessagePriority(int priorityValue) {
        this.priorityValue = priorityValue;
    }
}
