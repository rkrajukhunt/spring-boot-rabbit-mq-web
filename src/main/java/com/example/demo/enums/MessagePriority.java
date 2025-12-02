package com.example.demo.enums;

import lombok.Getter;

import java.util.List;

@Getter
public enum MessagePriority {
    HIGH("priority.high", 10, List.of(
            "inappcommunication.priority-high-1-fed",
            "inappcommunication.priority-high-2-fed"
    ), List.of("priority.high.1", "priority.high.2")),
    MEDIUM("priority.medium", 5, List.of(
            "inappcommunication.priority-medium-1-fed",
            "inappcommunication.priority-medium-2-fed"
    ), List.of("priority.medium.1", "priority.medium.2")),
    LOW("priority.low", 1, List.of(
            "inappcommunication.priority-low-1-fed",
            "inappcommunication.priority-low-2-fed"
    ), List.of("priority.low.1", "priority.low.2"));

    private final String routingKeyPrefix;
    private final int priorityValue;
    private final List<String> queueNames;
    private final List<String> routingKeys;

    MessagePriority(String routingKeyPrefix, int priorityValue, List<String> queueNames, List<String> routingKeys) {
        this.routingKeyPrefix = routingKeyPrefix;
        this.priorityValue = priorityValue;
        this.queueNames = queueNames;
        this.routingKeys = routingKeys;
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

    /**
     * Get routing key for specific queue index
     */
    public String getRoutingKey(int index) {
        return routingKeys.get(index % routingKeys.size());
    }

    /**
     * Get routing key for queue name
     */
    public String getRoutingKeyForQueue(String queueName) {
        int index = queueNames.indexOf(queueName);
        return index >= 0 ? routingKeys.get(index) : routingKeys.get(0);
    }
}
