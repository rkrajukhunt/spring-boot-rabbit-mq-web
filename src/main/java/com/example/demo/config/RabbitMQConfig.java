package com.example.demo.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class RabbitMQConfig {

    @Value("${app.rabbitmq.exchange.name}")
    private String exchangeName;

    @Value("${app.rabbitmq.exchange.dlx-name}")
    private String dlxExchangeName;

    @Value("${app.rabbitmq.queue.ttl:86400000}")
    private long queueTtl; // 24 hours default

    // ========== EXCHANGES ==========

    @Bean
    public DirectExchange messageExchange() {
        return new DirectExchange(exchangeName, true, false);
    }

    @Bean
    public DirectExchange dlxExchange() {
        return new DirectExchange(dlxExchangeName, true, false);
    }

    // ========== MAIN PRIORITY QUEUE ==========

    /**
     * Single priority queue with x-max-priority=10
     * RabbitMQ native priority support (0-10, where 10 is highest)
     * Supports millions of messages with efficient priority handling
     */
    @Bean
    public Queue messagePriorityQueue() {
        return createMainQueue("inappcommunication.messages-fed", "dlq.messages");
    }

    // ========== RETRY QUEUES ==========

    /**
     * Retry queues with exponential backoff (2s, 4s, 8s)
     * Messages from these queues are routed back to main queue with same priority
     */
    @Bean
    public Queue retryQueueLevel1() {
        return createRetryQueue("inappcommunication.messages-retry-1-fed", 2000, "messages.priority");
    }

    @Bean
    public Queue retryQueueLevel2() {
        return createRetryQueue("inappcommunication.messages-retry-2-fed", 4000, "messages.priority");
    }

    @Bean
    public Queue retryQueueLevel3() {
        return createRetryQueue("inappcommunication.messages-retry-3-fed", 8000, "messages.priority");
    }

    // ========== DEAD LETTER QUEUE (DLQ) ==========

    /**
     * Single DLQ for messages that exhausted all retries
     */
    @Bean
    public Queue dlqQueue() {
        return createDLQ("inappcommunication.messages-dlq-fed");
    }

    // ========== BINDINGS ==========

    /**
     * Bind main priority queue to exchange
     */
    @Bean
    public Binding mainQueueBinding() {
        return BindingBuilder
                .bind(messagePriorityQueue())
                .to(messageExchange())
                .with("messages.priority");
    }

    /**
     * Bind DLQ to DLX exchange
     */
    @Bean
    public Binding dlqBinding() {
        return BindingBuilder
                .bind(dlqQueue())
                .to(dlxExchange())
                .with("dlq.messages");
    }

    // ========== HELPER METHODS ==========

    /**
     * Create the main priority queue with:
     * - x-max-priority: 10 (RabbitMQ native priority support, 0-10 scale)
     * - Quorum type for production reliability
     * - Lazy mode for high throughput
     * - 24-hour TTL
     * - DLX configuration for failed messages
     */
    private Queue createMainQueue(String queueName, String dlqRoutingKey) {
        Map<String, Object> args = new HashMap<>();
        args.put("x-queue-type", "quorum"); // Quorum queue for production reliability
        args.put("x-max-priority", 10); // Enable priority 0-10 (10=highest)
        args.put("x-queue-mode", "lazy"); // Store messages on disk for high throughput
        args.put("x-message-ttl", queueTtl); // 24-hour TTL
        args.put("x-dead-letter-exchange", dlxExchangeName);
        args.put("x-dead-letter-routing-key", dlqRoutingKey);
        return new Queue(queueName, true, false, false, args);
    }

    /**
     * Create a retry queue with TTL and DLX back to main exchange
     * Retry queues also support priority to maintain message priority after retry
     */
    private Queue createRetryQueue(String queueName, int ttlMs, String routingKey) {
        Map<String, Object> args = new HashMap<>();
        args.put("x-queue-type", "quorum"); // Quorum queue for production reliability
        args.put("x-max-priority", 10); // Maintain priority through retry
        args.put("x-message-ttl", ttlMs);
        args.put("x-dead-letter-exchange", exchangeName);
        args.put("x-dead-letter-routing-key", routingKey);
        return new Queue(queueName, true, false, false, args);
    }

    /**
     * Create a Dead Letter Queue (DLQ) with Quorum type and priority support
     */
    private Queue createDLQ(String queueName) {
        Map<String, Object> args = new HashMap<>();
        args.put("x-queue-type", "quorum"); // Quorum queue for production reliability
        args.put("x-max-priority", 10); // Keep priority info in DLQ for analysis
        return new Queue(queueName, true, false, false, args);
    }

    // ========== MESSAGE CONVERTER ==========

    @Bean
    public Jackson2JsonMessageConverter jackson2JsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // ========== RABBIT ADMIN (for queue management) ==========

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    // ========== RABBIT TEMPLATE (Publisher) ==========

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jackson2JsonMessageConverter());
        template.setMandatory(true);

        // Publisher confirms callback
        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (correlationData != null) {
                if (ack) {
                    log.debug("Message with correlation ID {} confirmed by broker", correlationData.getId());
                } else {
                    log.error("Message with correlation ID {} was rejected by broker. Cause: {}",
                            correlationData.getId(), cause);
                }
            }
        });

        // Return callback for unroutable messages
        template.setReturnsCallback(returned -> {
            log.error("Message returned: {} - Reply Code: {} - Reply Text: {} - Exchange: {} - Routing Key: {}",
                    returned.getMessage(),
                    returned.getReplyCode(),
                    returned.getReplyText(),
                    returned.getExchange(),
                    returned.getRoutingKey());
        });

        return template;
    }

    // ========== LISTENER CONTAINER FACTORY (Consumer) ==========

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory =
                new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jackson2JsonMessageConverter());

        // Concurrency: 10-20 threads per queue
        factory.setConcurrentConsumers(10);
        factory.setMaxConcurrentConsumers(20);

        // Prefetch: 50 messages per consumer
        factory.setPrefetchCount(50);

        // Manual acknowledgment for reliability
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);

        // Default requeue on failure = false (we handle retries manually)
        factory.setDefaultRequeueRejected(false);

        log.info("RabbitMQ Listener Container Factory configured: 6 queues (2 per priority), concurrency: 10-20, prefetch: 50, manual ACK");

        return factory;
    }
}
