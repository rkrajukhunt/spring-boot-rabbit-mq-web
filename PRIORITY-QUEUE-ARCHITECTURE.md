# RabbitMQ Native Priority Queue Architecture

## Overview

This system has been refactored to use **RabbitMQ's native priority queue feature** (https://www.rabbitmq.com/docs/priority) instead of managing separate queues for each priority level. This provides a simpler, more elegant, and more scalable solution for multi-pod deployments.

## Key Changes

### Before (6 Separate Queues)
- 6 main queues (high-1, high-2, medium-1, medium-2, low-1, low-2)
- 18 retry queues (3 per main queue)
- 6 DLQs
- Complex load balancing logic
- **Total: 30 queues**

### After (Single Priority Queue)
- 1 main priority queue with `x-max-priority=10`
- 3 retry queues (shared across all priorities)
- 1 DLQ
- No load balancing needed - RabbitMQ handles it
- **Total: 5 queues**

## Architecture

### Priority Queue Configuration

The main queue `inappcommunication.messages-fed` is configured with:

```java
args.put("x-queue-type", "quorum");      // Production reliability
args.put("x-max-priority", 10);          // Priority support (0-10, 10=highest)
args.put("x-queue-mode", "lazy");        // High throughput
args.put("x-message-ttl", 86400000);     // 24-hour TTL
```

### Priority Levels

| Priority Level | Value | Use Case |
|---------------|-------|----------|
| HIGH          | 10    | Urgent messages, processed first |
| MEDIUM        | 5     | Default priority for normal messages |
| LOW           | 1     | Background tasks, processed last |

### Message Flow

```
┌──────────────────────────────────────────────────────────────────┐
│  Producer (API)                                                   │
│  - Publishes message with priority (10, 5, or 1)                 │
│  - Single routing key: "messages.priority"                       │
└─────────────────────────────┬────────────────────────────────────┘
                              │
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  Exchange: inappcommunication.communication-fed (Direct)         │
└─────────────────────────────┬────────────────────────────────────┘
                              │
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  Priority Queue: inappcommunication.messages-fed                 │
│  - x-max-priority: 10                                            │
│  - RabbitMQ automatically orders messages by priority            │
│  - HIGH priority messages delivered first                        │
│  - Messages distributed to all consumers across all pods         │
└─────────────────────────────┬────────────────────────────────────┘
                              │
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  Consumers (50-100 per pod)                                      │
│  - Pod 1: 50-100 consumer threads                                │
│  - Pod 2: 50-100 consumer threads                                │
│  - Pod N: 50-100 consumer threads                                │
│  - All pods compete fairly for messages                          │
│  - Priority order maintained across all pods                     │
└──────────────────────────────────────────────────────────────────┘

On Failure:
└──→ Retry Queue (3 levels: 2s, 4s, 8s) → Back to Main Queue
     After 3 Retries → DLQ (inappcommunication.messages-dlq-fed)
```

## Multi-Pod Deployment

### How It Works

1. **Single Queue, Multiple Consumers**
   - All pods connect to the same priority queue
   - Each pod runs 50-100 consumer threads
   - RabbitMQ distributes messages across ALL consumers

2. **Load Distribution**
   - RabbitMQ uses round-robin distribution among consumers
   - If Pod 1 has 100 consumers and Pod 2 has 100 consumers, RabbitMQ sees 200 total consumers
   - Messages are distributed evenly: ~50% to Pod 1, ~50% to Pod 2

3. **Priority Maintained**
   - RabbitMQ delivers HIGH priority messages first
   - Within each priority level, messages are distributed fairly
   - If queue has 100 HIGH and 100 LOW messages, consumers get HIGH messages first

4. **Horizontal Scaling**
   ```
   1 pod  × 100 consumers = 100 total consumers
   2 pods × 100 consumers = 200 total consumers (2x throughput)
   3 pods × 100 consumers = 300 total consumers (3x throughput)
   N pods × 100 consumers = N × 100 consumers (linear scaling)
   ```

### Benefits Over Separate Queues

✅ **Simpler Architecture**
- Single queue instead of 6 queues
- No need for load balancing logic
- Easier to monitor and manage

✅ **Better Multi-Pod Support**
- RabbitMQ natively distributes messages across all pods
- No queue affinity issues
- Truly independent pod scaling

✅ **Automatic Load Balancing**
- RabbitMQ handles message distribution
- No custom load balancing code needed
- Fair distribution guaranteed

✅ **Priority is Native**
- RabbitMQ handles priority ordering
- More efficient than separate queues
- Priorities maintained across all pods

✅ **Easier Scaling**
- Add more pods = more consumers automatically
- No queue rebalancing needed
- Linear scaling guaranteed

## Capacity & Performance

### Single Pod

- **Consumers**: 50-100 threads
- **Processing time 1s**: 100 msgs/sec = 6,000/min = 360,000/hour = **8.6M/day**
- **Processing time 5s**: 20 msgs/sec = 1,200/min = 72,000/hour = **1.7M/day**

### Three Pods (Typical Production)

- **Consumers**: 150-300 threads (3 × 50-100)
- **Processing time 1s**: 300 msgs/sec = 18,000/min = 1.08M/hour = **25.9M/day**
- **Processing time 5s**: 60 msgs/sec = 3,600/min = 216,000/hour = **5.2M/day**

### Scalability

| Pods | Consumers | Throughput (1s processing) | Throughput (5s processing) |
|------|-----------|---------------------------|---------------------------|
| 1    | 50-100    | 8.6M/day                  | 1.7M/day                  |
| 2    | 100-200   | 17.3M/day                 | 3.5M/day                  |
| 3    | 150-300   | 25.9M/day                 | 5.2M/day                  |
| 5    | 250-500   | 43.2M/day                 | 8.6M/day                  |
| 10   | 500-1000  | 86.4M/day                 | 17.3M/day                 |

## Configuration

### Producer Configuration

```java
// Publish with priority
rabbitTemplate.convertAndSend(
    exchangeName,
    "messages.priority",
    payload,
    message -> {
        message.getMessageProperties().setPriority(priority.getPriorityValue());
        message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        return message;
    }
);
```

### Consumer Configuration

```java
@RabbitListener(
    queues = "inappcommunication.messages-fed",
    concurrency = "50-100",
    containerFactory = "rabbitListenerContainerFactory"
)
public void consumePriorityQueue(@Payload MessagePayload payload, ...) {
    // Process message
    // RabbitMQ already delivered this in priority order
}
```

### Application Configuration

```yaml
spring:
  rabbitmq:
    cache:
      connection:
        size: 1  # One connection per pod
      channel:
        size: 120  # Enough channels for all consumers
    listener:
      simple:
        concurrency: 50      # Min consumers per pod
        max-concurrency: 100 # Max consumers per pod
        prefetch: 50
```

## Queue Topology

### Queues

| Queue Name | Type | Priority | TTL | Purpose |
|-----------|------|----------|-----|---------|
| `inappcommunication.messages-fed` | Quorum | 0-10 | 24h | Main priority queue |
| `inappcommunication.messages-retry-1-fed` | Quorum | 0-10 | 2s | First retry (2s delay) |
| `inappcommunication.messages-retry-2-fed` | Quorum | 0-10 | 4s | Second retry (4s delay) |
| `inappcommunication.messages-retry-3-fed` | Quorum | 0-10 | 8s | Third retry (8s delay) |
| `inappcommunication.messages-dlq-fed` | Quorum | 0-10 | ∞ | Dead letter queue |

### Exchanges

| Exchange Name | Type | Purpose |
|--------------|------|---------|
| `inappcommunication.communication-fed` | Direct | Main exchange for messages |
| `inappcommunication.communication-dlx-fed` | Direct | Dead letter exchange |

## Monitoring

### Key Metrics

1. **Queue Depth**
   - Monitor: `inappcommunication.messages-fed` message count
   - Alert if > 10,000 messages
   - Indicates backlog or slow consumers

2. **Consumer Count**
   - Expected: 50-100 per pod
   - Alert if < 50 per pod (degraded)
   - Verifies all pods connected

3. **Priority Distribution**
   - Monitor messages by priority in queue
   - Verify HIGH messages processed first
   - Track average priority of processed messages

4. **DLQ Depth**
   - Monitor: `inappcommunication.messages-dlq-fed`
   - Alert on any messages (indicates failures)
   - Track by original priority

### Health Check Endpoints

- `/actuator/health` - Overall health
- `/actuator/rabbitmq/metrics` - Comprehensive metrics
- `/actuator/rabbitmq/queue-depths` - Queue message counts
- `/actuator/rabbitmq/consumer-counts` - Active consumers
- `/actuator/prometheus` - Prometheus metrics export

## Testing Priority Behavior

### Test Scenario: Backlog with Mixed Priorities

1. Stop all consumers
2. Publish messages in this order:
   ```
   LOW    (priority=1)
   MEDIUM (priority=5)
   LOW    (priority=1)
   HIGH   (priority=10)
   MEDIUM (priority=5)
   LOW    (priority=1)
   ```
3. Start consumers
4. Verify processing order:
   ```
   HIGH   (priority=10)  ← Processed FIRST
   MEDIUM (priority=5)   ← Processed second
   MEDIUM (priority=5)   ← Processed third
   LOW    (priority=1)   ← Processed fourth
   LOW    (priority=1)   ← Processed fifth
   LOW    (priority=1)   ← Processed last
   ```

### Expected Behavior

- **With backlog**: HIGH messages always delivered before LOW
- **Without backlog**: Messages delivered in arrival order (no backlog = no reordering needed)
- **Across pods**: Priority maintained regardless of which pod's consumer receives the message

## Migration from 6-Queue Architecture

### Step 1: Deploy New Version
- Deploy pods with single priority queue configuration
- Old queues remain untouched

### Step 2: Drain Old Queues
- Stop publishing to old 6-queue system
- Let consumers drain old queues
- Monitor until all old queues empty

### Step 3: Start Publishing to New Queue
- Switch publisher to new single queue
- Messages automatically distributed to all pods

### Step 4: Clean Up
- Delete old 6-queue configuration
- Remove old monitoring dashboards
- Update documentation

## Best Practices

### 1. Priority Assignment

✅ **DO:**
- Use HIGH (10) for urgent, time-sensitive messages
- Use MEDIUM (5) as default for normal operations
- Use LOW (1) for background tasks, analytics, reports

❌ **DON'T:**
- Mark everything as HIGH (defeats the purpose)
- Use fine-grained priorities (1-10 range is plenty)
- Change priority mid-flight (create new message instead)

### 2. Consumer Configuration

✅ **DO:**
- Set `concurrency: 50-100` for good throughput
- Use `prefetch: 50` to balance latency and throughput
- Enable manual acknowledgment for reliability

❌ **DON'T:**
- Set concurrency too low (< 20 per pod)
- Use auto-ack mode (loses messages on crash)
- Set prefetch too high (> 100, causes uneven distribution)

### 3. Monitoring

✅ **DO:**
- Alert on queue depth > 10,000
- Alert on consumer count < expected
- Monitor DLQ for any messages
- Track processing time by priority

❌ **DON'T:**
- Ignore DLQ messages
- Assume priority is working (verify with metrics)
- Forget to monitor across all pods

## Troubleshooting

### Problem: Messages not processed in priority order

**Cause**: No backlog - when queue is empty, messages processed as they arrive

**Solution**: This is normal! Priority only matters when there's a backlog.

### Problem: One pod getting all messages

**Cause**: Other pods not connected or consumers not started

**Check**:
1. Verify consumer count per pod
2. Check pod logs for connection errors
3. Verify all pods have same configuration

### Problem: Low throughput despite multiple pods

**Cause**: Consumer concurrency too low

**Solution**:
1. Increase `concurrency` to 50-100
2. Monitor CPU/memory usage
3. Scale pods if needed

### Problem: Messages stuck in queue

**Cause**: Consumers crashing or processing taking too long

**Check**:
1. Consumer logs for errors
2. Processing time metrics
3. Resource limits (CPU, memory)

## Summary

✅ **Simpler**: 5 queues instead of 30
✅ **Scalable**: Linear scaling with pod count
✅ **Native**: RabbitMQ handles priority and distribution
✅ **Multi-Pod**: Perfect for Kubernetes deployments
✅ **Production-Ready**: Quorum queues, TLS, monitoring, federated

This architecture is ideal for:
- **Multi-pod Kubernetes deployments**
- **High-throughput systems** (millions of messages/day)
- **Priority-based processing** (urgent vs background)
- **Horizontal scaling** (add pods = more throughput)
- **Production environments** (reliability, monitoring, federation)
