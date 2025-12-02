package com.example.demo.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
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

    // ========== EXCHANGES ==========

    @Bean
    public TopicExchange messageExchange() {
        return new TopicExchange(exchangeName, true, false);
    }

    @Bean
    public TopicExchange dlxExchange() {
        return new TopicExchange(dlxExchangeName, true, false);
    }

    // ========== MAIN QUEUES (3 priority-based queues) ==========

    @Bean
    public Queue messageQueue1() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-queue-mode", "lazy"); // Store messages on disk for high throughput
        args.put("x-dead-letter-exchange", dlxExchangeName);
        args.put("x-dead-letter-routing-key", "dlq.message-queue-1");
        return new Queue("message-queue-1", true, false, false, args);
    }

    @Bean
    public Queue messageQueue2() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-queue-mode", "lazy");
        args.put("x-dead-letter-exchange", dlxExchangeName);
        args.put("x-dead-letter-routing-key", "dlq.message-queue-2");
        return new Queue("message-queue-2", true, false, false, args);
    }

    @Bean
    public Queue messageQueue3() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-queue-mode", "lazy");
        args.put("x-dead-letter-exchange", dlxExchangeName);
        args.put("x-dead-letter-routing-key", "dlq.message-queue-3");
        return new Queue("message-queue-3", true, false, false, args);
    }

    // ========== RETRY QUEUES - Queue 1 (High Priority) ==========

    @Bean
    public Queue retryQueue1Level1() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 2000); // 2 seconds
        args.put("x-dead-letter-exchange", exchangeName);
        args.put("x-dead-letter-routing-key", "message.priority.high");
        return new Queue("message-queue-1.retry.1", true, false, false, args);
    }

    @Bean
    public Queue retryQueue1Level2() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 4000); // 4 seconds
        args.put("x-dead-letter-exchange", exchangeName);
        args.put("x-dead-letter-routing-key", "message.priority.high");
        return new Queue("message-queue-1.retry.2", true, false, false, args);
    }

    @Bean
    public Queue retryQueue1Level3() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 8000); // 8 seconds
        args.put("x-dead-letter-exchange", exchangeName);
        args.put("x-dead-letter-routing-key", "message.priority.high");
        return new Queue("message-queue-1.retry.3", true, false, false, args);
    }

    // ========== RETRY QUEUES - Queue 2 (Medium Priority) ==========

    @Bean
    public Queue retryQueue2Level1() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 2000);
        args.put("x-dead-letter-exchange", exchangeName);
        args.put("x-dead-letter-routing-key", "message.priority.medium");
        return new Queue("message-queue-2.retry.1", true, false, false, args);
    }

    @Bean
    public Queue retryQueue2Level2() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 4000);
        args.put("x-dead-letter-exchange", exchangeName);
        args.put("x-dead-letter-routing-key", "message.priority.medium");
        return new Queue("message-queue-2.retry.2", true, false, false, args);
    }

    @Bean
    public Queue retryQueue2Level3() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 8000);
        args.put("x-dead-letter-exchange", exchangeName);
        args.put("x-dead-letter-routing-key", "message.priority.medium");
        return new Queue("message-queue-2.retry.3", true, false, false, args);
    }

    // ========== RETRY QUEUES - Queue 3 (Low Priority) ==========

    @Bean
    public Queue retryQueue3Level1() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 2000);
        args.put("x-dead-letter-exchange", exchangeName);
        args.put("x-dead-letter-routing-key", "message.priority.low");
        return new Queue("message-queue-3.retry.1", true, false, false, args);
    }

    @Bean
    public Queue retryQueue3Level2() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 4000);
        args.put("x-dead-letter-exchange", exchangeName);
        args.put("x-dead-letter-routing-key", "message.priority.low");
        return new Queue("message-queue-3.retry.2", true, false, false, args);
    }

    @Bean
    public Queue retryQueue3Level3() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 8000);
        args.put("x-dead-letter-exchange", exchangeName);
        args.put("x-dead-letter-routing-key", "message.priority.low");
        return new Queue("message-queue-3.retry.3", true, false, false, args);
    }

    // ========== DEAD LETTER QUEUES (DLQ) ==========

    @Bean
    public Queue dlqQueue1() {
        return new Queue("message-queue-1.dlq", true);
    }

    @Bean
    public Queue dlqQueue2() {
        return new Queue("message-queue-2.dlq", true);
    }

    @Bean
    public Queue dlqQueue3() {
        return new Queue("message-queue-3.dlq", true);
    }

    // ========== BINDINGS - Main Queues ==========

    @Bean
    public Binding binding1() {
        return BindingBuilder
                .bind(messageQueue1())
                .to(messageExchange())
                .with("message.priority.high");
    }

    @Bean
    public Binding binding2() {
        return BindingBuilder
                .bind(messageQueue2())
                .to(messageExchange())
                .with("message.priority.medium");
    }

    @Bean
    public Binding binding3() {
        return BindingBuilder
                .bind(messageQueue3())
                .to(messageExchange())
                .with("message.priority.low");
    }

    // ========== BINDINGS - DLQ ==========

    @Bean
    public Binding dlqBinding1() {
        return BindingBuilder
                .bind(dlqQueue1())
                .to(dlxExchange())
                .with("dlq.message-queue-1");
    }

    @Bean
    public Binding dlqBinding2() {
        return BindingBuilder
                .bind(dlqQueue2())
                .to(dlxExchange())
                .with("dlq.message-queue-2");
    }

    @Bean
    public Binding dlqBinding3() {
        return BindingBuilder
                .bind(dlqQueue3())
                .to(dlxExchange())
                .with("dlq.message-queue-3");
    }

    // ========== MESSAGE CONVERTER ==========

    @Bean
    public Jackson2JsonMessageConverter jackson2JsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
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

        log.info("RabbitMQ Listener Container Factory configured with concurrency: 10-20, prefetch: 50, manual ACK");

        return factory;
    }
}
