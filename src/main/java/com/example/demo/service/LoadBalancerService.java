package com.example.demo.service;

import com.example.demo.enums.MessagePriority;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
@RequiredArgsConstructor
public class LoadBalancerService {

    private final RabbitAdmin rabbitAdmin;

    // Round-robin counters for each priority
    private final AtomicInteger highCounter = new AtomicInteger(0);
    private final AtomicInteger mediumCounter = new AtomicInteger(0);
    private final AtomicInteger lowCounter = new AtomicInteger(0);

    /**
     * Select the best queue for a given priority using load balancing strategies
     * Strategy: Check queue depths and select the one with fewer messages
     * Fallback: Round-robin if queue info not available
     */
    public String selectQueue(MessagePriority priority) {
        List<String> queues = priority.getAllQueues();

        try {
            // Try to get queue with lowest message count
            String selectedQueue = selectByQueueDepth(queues);
            if (selectedQueue != null) {
                log.debug("Selected queue {} for priority {} based on queue depth", selectedQueue, priority);
                return selectedQueue;
            }
        } catch (Exception e) {
            log.warn("Failed to get queue depths, falling back to round-robin: {}", e.getMessage());
        }

        // Fallback: Round-robin
        String selectedQueue = selectByRoundRobin(priority, queues);
        log.debug("Selected queue {} for priority {} using round-robin", selectedQueue, priority);
        return selectedQueue;
    }

    /**
     * Select queue with lowest message count (queue depth)
     */
    private String selectByQueueDepth(List<String> queues) {
        String selectedQueue = null;
        int minDepth = Integer.MAX_VALUE;

        for (String queueName : queues) {
            try {
                Properties props = rabbitAdmin.getQueueProperties(queueName);
                if (props != null) {
                    Integer messageCount = (Integer) props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
                    if (messageCount != null && messageCount < minDepth) {
                        minDepth = messageCount;
                        selectedQueue = queueName;
                    }
                }
            } catch (Exception e) {
                log.trace("Could not get queue info for {}: {}", queueName, e.getMessage());
            }
        }

        return selectedQueue;
    }

    /**
     * Round-robin selection across queues
     */
    private String selectByRoundRobin(MessagePriority priority, List<String> queues) {
        AtomicInteger counter = getCounterForPriority(priority);
        int index = counter.getAndIncrement() % queues.size();
        return queues.get(index);
    }

    /**
     * Get the appropriate counter for the priority level
     */
    private AtomicInteger getCounterForPriority(MessagePriority priority) {
        return switch (priority) {
            case HIGH -> highCounter;
            case MEDIUM -> mediumCounter;
            case LOW -> lowCounter;
        };
    }

    /**
     * Get queue statistics for monitoring
     */
    public QueueStats getQueueStats(String queueName) {
        try {
            Properties props = rabbitAdmin.getQueueProperties(queueName);
            if (props != null) {
                Integer messageCount = (Integer) props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
                Integer consumerCount = (Integer) props.get(RabbitAdmin.QUEUE_CONSUMER_COUNT);
                return new QueueStats(queueName, messageCount, consumerCount);
            }
        } catch (Exception e) {
            log.error("Failed to get stats for queue {}", queueName, e);
        }
        return new QueueStats(queueName, 0, 0);
    }

    /**
     * Queue statistics holder
     */
    public record QueueStats(String queueName, int messageCount, int consumerCount) {
    }
}
