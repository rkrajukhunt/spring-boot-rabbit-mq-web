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

    // ========== MAIN QUEUES (6 queues: 2 per priority level) ==========

    // HIGH PRIORITY QUEUES
    @Bean
    public Queue messageQueueHigh1() {
        return createMainQueue("inappcommunication.priority-high-1-fed", "dlq.high-1");
    }

    @Bean
    public Queue messageQueueHigh2() {
        return createMainQueue("inappcommunication.priority-high-2-fed", "dlq.high-2");
    }

    // MEDIUM PRIORITY QUEUES
    @Bean
    public Queue messageQueueMedium1() {
        return createMainQueue("inappcommunication.priority-medium-1-fed", "dlq.medium-1");
    }

    @Bean
    public Queue messageQueueMedium2() {
        return createMainQueue("inappcommunication.priority-medium-2-fed", "dlq.medium-2");
    }

    // LOW PRIORITY QUEUES
    @Bean
    public Queue messageQueueLow1() {
        return createMainQueue("inappcommunication.priority-low-1-fed", "dlq.low-1");
    }

    @Bean
    public Queue messageQueueLow2() {
        return createMainQueue("inappcommunication.priority-low-2-fed", "dlq.low-2");
    }

    // ========== RETRY QUEUES - HIGH PRIORITY ==========

    @Bean
    public Queue retryQueueHigh1Level1() {
        return createRetryQueue("inappcommunication.priority-high-1-retry-1-fed", 2000, "priority.high.1");
    }

    @Bean
    public Queue retryQueueHigh1Level2() {
        return createRetryQueue("inappcommunication.priority-high-1-retry-2-fed", 4000, "priority.high.1");
    }

    @Bean
    public Queue retryQueueHigh1Level3() {
        return createRetryQueue("inappcommunication.priority-high-1-retry-3-fed", 8000, "priority.high.1");
    }

    @Bean
    public Queue retryQueueHigh2Level1() {
        return createRetryQueue("inappcommunication.priority-high-2-retry-1-fed", 2000, "priority.high.2");
    }

    @Bean
    public Queue retryQueueHigh2Level2() {
        return createRetryQueue("inappcommunication.priority-high-2-retry-2-fed", 4000, "priority.high.2");
    }

    @Bean
    public Queue retryQueueHigh2Level3() {
        return createRetryQueue("inappcommunication.priority-high-2-retry-3-fed", 8000, "priority.high.2");
    }

    // ========== RETRY QUEUES - MEDIUM PRIORITY ==========

    @Bean
    public Queue retryQueueMedium1Level1() {
        return createRetryQueue("inappcommunication.priority-medium-1-retry-1-fed", 2000, "priority.medium.1");
    }

    @Bean
    public Queue retryQueueMedium1Level2() {
        return createRetryQueue("inappcommunication.priority-medium-1-retry-2-fed", 4000, "priority.medium.1");
    }

    @Bean
    public Queue retryQueueMedium1Level3() {
        return createRetryQueue("inappcommunication.priority-medium-1-retry-3-fed", 8000, "priority.medium.1");
    }

    @Bean
    public Queue retryQueueMedium2Level1() {
        return createRetryQueue("inappcommunication.priority-medium-2-retry-1-fed", 2000, "priority.medium.2");
    }

    @Bean
    public Queue retryQueueMedium2Level2() {
        return createRetryQueue("inappcommunication.priority-medium-2-retry-2-fed", 4000, "priority.medium.2");
    }

    @Bean
    public Queue retryQueueMedium2Level3() {
        return createRetryQueue("inappcommunication.priority-medium-2-retry-3-fed", 8000, "priority.medium.2");
    }

    // ========== RETRY QUEUES - LOW PRIORITY ==========

    @Bean
    public Queue retryQueueLow1Level1() {
        return createRetryQueue("inappcommunication.priority-low-1-retry-1-fed", 2000, "priority.low.1");
    }

    @Bean
    public Queue retryQueueLow1Level2() {
        return createRetryQueue("inappcommunication.priority-low-1-retry-2-fed", 4000, "priority.low.1");
    }

    @Bean
    public Queue retryQueueLow1Level3() {
        return createRetryQueue("inappcommunication.priority-low-1-retry-3-fed", 8000, "priority.low.1");
    }

    @Bean
    public Queue retryQueueLow2Level1() {
        return createRetryQueue("inappcommunication.priority-low-2-retry-1-fed", 2000, "priority.low.2");
    }

    @Bean
    public Queue retryQueueLow2Level2() {
        return createRetryQueue("inappcommunication.priority-low-2-retry-2-fed", 4000, "priority.low.2");
    }

    @Bean
    public Queue retryQueueLow2Level3() {
        return createRetryQueue("inappcommunication.priority-low-2-retry-3-fed", 8000, "priority.low.2");
    }

    // ========== DEAD LETTER QUEUES (DLQ) ==========

    @Bean
    public Queue dlqQueueHigh1() {
        return createDLQ("inappcommunication.priority-high-1-dlq-fed");
    }

    @Bean
    public Queue dlqQueueHigh2() {
        return createDLQ("inappcommunication.priority-high-2-dlq-fed");
    }

    @Bean
    public Queue dlqQueueMedium1() {
        return createDLQ("inappcommunication.priority-medium-1-dlq-fed");
    }

    @Bean
    public Queue dlqQueueMedium2() {
        return createDLQ("inappcommunication.priority-medium-2-dlq-fed");
    }

    @Bean
    public Queue dlqQueueLow1() {
        return createDLQ("inappcommunication.priority-low-1-dlq-fed");
    }

    @Bean
    public Queue dlqQueueLow2() {
        return createDLQ("inappcommunication.priority-low-2-dlq-fed");
    }

    // ========== BINDINGS - Main Queues to Exchange ==========

    @Bean
    public Binding bindingHigh1() {
        return BindingBuilder
                .bind(messageQueueHigh1())
                .to(messageExchange())
                .with("priority.high.1");
    }

    @Bean
    public Binding bindingHigh2() {
        return BindingBuilder
                .bind(messageQueueHigh2())
                .to(messageExchange())
                .with("priority.high.2");
    }

    @Bean
    public Binding bindingMedium1() {
        return BindingBuilder
                .bind(messageQueueMedium1())
                .to(messageExchange())
                .with("priority.medium.1");
    }

    @Bean
    public Binding bindingMedium2() {
        return BindingBuilder
                .bind(messageQueueMedium2())
                .to(messageExchange())
                .with("priority.medium.2");
    }

    @Bean
    public Binding bindingLow1() {
        return BindingBuilder
                .bind(messageQueueLow1())
                .to(messageExchange())
                .with("priority.low.1");
    }

    @Bean
    public Binding bindingLow2() {
        return BindingBuilder
                .bind(messageQueueLow2())
                .to(messageExchange())
                .with("priority.low.2");
    }

    // ========== BINDINGS - DLQ to DLX Exchange ==========

    @Bean
    public Binding dlqBindingHigh1() {
        return BindingBuilder.bind(dlqQueueHigh1()).to(dlxExchange()).with("dlq.high-1");
    }

    @Bean
    public Binding dlqBindingHigh2() {
        return BindingBuilder.bind(dlqQueueHigh2()).to(dlxExchange()).with("dlq.high-2");
    }

    @Bean
    public Binding dlqBindingMedium1() {
        return BindingBuilder.bind(dlqQueueMedium1()).to(dlxExchange()).with("dlq.medium-1");
    }

    @Bean
    public Binding dlqBindingMedium2() {
        return BindingBuilder.bind(dlqQueueMedium2()).to(dlxExchange()).with("dlq.medium-2");
    }

    @Bean
    public Binding dlqBindingLow1() {
        return BindingBuilder.bind(dlqQueueLow1()).to(dlxExchange()).with("dlq.low-1");
    }

    @Bean
    public Binding dlqBindingLow2() {
        return BindingBuilder.bind(dlqQueueLow2()).to(dlxExchange()).with("dlq.low-2");
    }

    // ========== HELPER METHODS ==========

    /**
     * Create a main queue with Quorum type, lazy mode, TTL, and DLX configuration
     */
    private Queue createMainQueue(String queueName, String dlqRoutingKey) {
        Map<String, Object> args = new HashMap<>();
        args.put("x-queue-type", "quorum"); // Quorum queue for production reliability
        args.put("x-queue-mode", "lazy"); // Store messages on disk for high throughput
        args.put("x-message-ttl", queueTtl); // 24-hour TTL
        args.put("x-dead-letter-exchange", dlxExchangeName);
        args.put("x-dead-letter-routing-key", dlqRoutingKey);
        return new Queue(queueName, true, false, false, args);
    }

    /**
     * Create a retry queue with TTL and DLX back to main exchange
     */
    private Queue createRetryQueue(String queueName, int ttlMs, String routingKey) {
        Map<String, Object> args = new HashMap<>();
        args.put("x-queue-type", "quorum"); // Quorum queue for production reliability
        args.put("x-message-ttl", ttlMs);
        args.put("x-dead-letter-exchange", exchangeName);
        args.put("x-dead-letter-routing-key", routingKey);
        return new Queue(queueName, true, false, false, args);
    }

    /**
     * Create a Dead Letter Queue (DLQ) with Quorum type
     */
    private Queue createDLQ(String queueName) {
        Map<String, Object> args = new HashMap<>();
        args.put("x-queue-type", "quorum"); // Quorum queue for production reliability
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
