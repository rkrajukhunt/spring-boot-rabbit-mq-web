# Scaling Guide - Thread/Pod/Database Connection Alignment

## Overview

This document explains how to properly configure thread counts, pod counts, and database connections to prevent CTS failures and ensure optimal performance during load spikes (e.g., 8 AM batch operations).

---

## The Problem (Before Queue Implementation)

**Synchronous Processing Issues:**
- All message creation happens synchronously
- Multiple batch operations arrive simultaneously (common at 8 AM)
- System cannot control concurrent writes
- All available database connections used for writes, starving reads
- Results in CTS failures and user-facing errors
- No mechanism to gracefully handle load spikes

**Queue Solution:**
- RabbitMQ priority queue buffers incoming requests
- Controlled concurrency via consumer thread configuration
- Database connections properly allocated
- Graceful degradation under load

---

## Configuration Formula

### Critical Relationship

```
(Consumer Threads per Pod × Number of Pods) ≤ Maximum Database Connections
```

### Example Calculations

| Pods | Core Threads | Max Threads | DB Connections | Status |
|------|--------------|-------------|----------------|--------|
| 1    | 5            | 10          | 50             | ✅ Safe (5-10 ≤ 50) |
| 5    | 5            | 10          | 50             | ✅ Safe (25-50 ≤ 50) |
| 10   | 5            | 10          | 50             | ⚠️ Risky (50-100 > 50) |
| 10   | 3            | 5           | 50             | ✅ Safe (30-50 ≤ 50) |
| 15   | 3            | 3           | 50             | ✅ Safe (45 ≤ 50) |

---

## Configuration Files

### 1. Consumer Threads (application.yaml)

```yaml
app:
  messaging:
    async:
      executor:
        core-threads: 5      # Minimum consumer threads per pod
        max-threads: 10      # Maximum consumer threads per pod
```

**Key Points:**
- Each consumer thread uses one RabbitMQ channel
- Each thread may use one database connection during processing
- Threads scale from core-threads to max-threads based on load

### 2. Database Connections (application.yaml)

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 50    # Total connections available
      minimum-idle: 10         # Minimum idle connections
```

**Key Points:**
- HikariCP connection pool shared across all pods
- Each active thread can hold one connection
- Reserve connections for read operations (monitoring, tracking queries)

### 3. RabbitMQ Configuration (application.yaml)

```yaml
spring:
  rabbitmq:
    cache:
      connection:
        mode: channel          # One channel per consumer thread ✓
        size: 25              # Connection pool size
      channel:
        size: 50              # Channel pool per connection
```

**Key Points:**
- `mode: channel` ensures one channel per thread (REQUIRED)
- Channel pool size should match or exceed max consumer threads
- Connection pool handles all channels efficiently

---

## Scaling Scenarios

### Scenario 1: Small Deployment (1-3 pods)

**Recommendation:**
```yaml
# application.yaml
app:
  messaging:
    async:
      executor:
        core-threads: 10
        max-threads: 15

spring:
  datasource:
    hikari:
      maximum-pool-size: 50
```

**Calculations:**
- Max concurrent threads: 3 pods × 15 threads = 45 threads
- Database connections: 45 ≤ 50 ✅
- Throughput (1s processing): ~3.9M msgs/day

### Scenario 2: Medium Deployment (5-7 pods)

**Recommendation:**
```yaml
# application.yaml
app:
  messaging:
    async:
      executor:
        core-threads: 5
        max-threads: 7

spring:
  datasource:
    hikari:
      maximum-pool-size: 50
```

**Calculations:**
- Max concurrent threads: 7 pods × 7 threads = 49 threads
- Database connections: 49 ≤ 50 ✅
- Throughput (1s processing): ~4.2M msgs/day

### Scenario 3: Large Deployment (10+ pods)

**Recommendation:**
```yaml
# application.yaml
app:
  messaging:
    async:
      executor:
        core-threads: 3
        max-threads: 5

spring:
  datasource:
    hikari:
      maximum-pool-size: 50  # OR increase to 100
```

**Calculations:**
- Max concurrent threads: 10 pods × 5 threads = 50 threads
- Database connections: 50 ≤ 50 ✅
- Throughput (1s processing): ~4.3M msgs/day

**Alternative (Higher Throughput):**
```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 100  # Increased capacity

app:
  messaging:
    async:
      executor:
        core-threads: 5
        max-threads: 10
```

**Calculations:**
- Max concurrent threads: 10 pods × 10 threads = 100 threads
- Database connections: 100 ≤ 100 ✅
- Throughput (1s processing): ~8.6M msgs/day

---

## Handling Load Spikes (8 AM Problem)

### The Queue Solution

**Before (Synchronous):**
```
8:00 AM → 10,000 requests arrive simultaneously
       → 10,000 threads try to write to DB
       → Only 50 DB connections available
       → 9,950 requests fail or timeout
       → Users see errors
```

**After (Asynchronous Queue):**
```
8:00 AM → 10,000 requests arrive simultaneously
       → All accepted immediately (202 Accepted)
       → Messages queued in RabbitMQ with priority
       → Processed at controlled rate (5-10 threads per pod)
       → HIGH priority processed first
       → No database connection exhaustion
       → No user-facing errors
```

### Benefits

1. **Request Buffering**: Queue holds messages until resources available
2. **Controlled Concurrency**: Thread count limits DB connection usage
3. **Priority Handling**: HIGH priority messages processed first
4. **Graceful Degradation**: System stays responsive under load
5. **No CTS Failures**: Database connections properly managed

---

## Monitoring & Alerts

### Key Metrics to Monitor

1. **Queue Depth**
   - Endpoint: `/actuator/rabbitmq/queue-depth`
   - Alert if > 10,000 messages
   - Indicates processing slower than incoming rate

2. **Consumer Count**
   - Endpoint: `/actuator/rabbitmq/consumer-count`
   - Should be: pods × core-threads (minimum)
   - Alert if below expected

3. **Database Connections**
   - Monitor HikariCP metrics
   - Alert if utilization > 90%
   - Indicates need to scale or reduce threads

4. **Processing Duration**
   - Track via logs: `Successfully processed message X in Yms`
   - Alert if average > 5 seconds
   - Optimize business logic if needed

### Sample Alert Rules

```yaml
# Prometheus Alert Rules
groups:
  - name: rabbitmq_alerts
    rules:
      - alert: HighQueueDepth
        expr: rabbitmq_queue_messages > 10000
        annotations:
          summary: "Queue depth exceeds threshold"

      - alert: LowConsumerCount
        expr: rabbitmq_queue_consumers < 50
        annotations:
          summary: "Consumer count below expected"

      - alert: HighDBConnectionUtilization
        expr: hikari_connections_active / hikari_connections_max > 0.9
        annotations:
          summary: "Database connection pool near capacity"
```

---

## Configuration Checklist

### Before Deploying

- [ ] Verify thread count: `app.messaging.async.executor.core-threads`
- [ ] Verify pod count in deployment manifest
- [ ] Calculate: `(core-threads × pods) ≤ maximum-pool-size`
- [ ] Verify channel mode: `spring.rabbitmq.cache.connection.mode: channel`
- [ ] Configure batch acknowledgements: `app.rabbitmq.listener.batch-size`
- [ ] Set processing timeout: `app.messaging.async.processing.timeout-ms`
- [ ] Enable monitoring endpoints
- [ ] Configure alerts for queue depth and consumer count

### After Deploying

- [ ] Check consumer count: `curl /actuator/rabbitmq/consumer-count`
- [ ] Verify queue depth: `curl /actuator/rabbitmq/queue-depth`
- [ ] Monitor database connection usage
- [ ] Load test with expected peak load
- [ ] Verify no CTS failures under load
- [ ] Check logs for transaction errors

---

## Transaction Support

### Configuration

All message processing is wrapped in transactions:

```java
@Transactional
protected void processMessageInTransaction(
    MessagePayload payload,
    long deliveryTag,
    Channel channel,
    ExecutorService timeoutExecutor) throws IOException {

    // 1. Update tracking to PROCESSING
    trackingService.updateStatus(trackingId, MessageStatus.PROCESSING, null);

    // 2. Execute business logic
    communicationService.createCommunication(payload);

    // 3. Update tracking to COMPLETED
    trackingService.updateStatus(trackingId, MessageStatus.COMPLETED, null);

    // 4. Acknowledge message to RabbitMQ
    channel.basicAck(deliveryTag, false);
}
```

### Atomicity Guarantee

- Either **ALL** succeed: business logic + database updates + ACK
- Or **ALL** fail: transaction rolls back, message goes to retry queue
- No partial completion possible

---

## Batch Acknowledgements

### When to Enable

```yaml
app:
  rabbitmq:
    listener:
      batch-size: 10              # ACK every 10 messages
      enable-batch-ack: true      # Enable batch ACK

spring:
  rabbitmq:
    listener:
      simple:
        prefetch: 50              # Must be > 1 for batch ACK
```

### How It Works

1. Consumer fetches messages (prefetch: 50)
2. Processes message 1 → deferred ACK
3. Processes message 2 → deferred ACK
4. ...
5. Processes message 10 → **batch ACK messages 1-10**
6. Repeat

### Benefits

- Reduces network round-trips (1 ACK instead of 10)
- Improves throughput by 15-30%
- Safe with manual ACK mode + transactions

---

## Troubleshooting

### Problem: CTS Failures

**Symptoms:**
- User-facing errors during load spikes
- Database connection timeout errors
- "No available connections" in logs

**Solution:**
1. Check: `(threads × pods) ≤ DB connections`
2. Reduce threads per pod, or
3. Increase database pool size, or
4. Reduce pod count

### Problem: High Queue Depth

**Symptoms:**
- Queue depth > 10,000 messages
- Processing slower than incoming rate
- Users reporting delays

**Solution:**
1. Increase consumer threads (if DB allows)
2. Add more pods
3. Optimize business logic (`createCommunication`)
4. Check for database bottlenecks

### Problem: Low Throughput

**Symptoms:**
- Processing rate slower than expected
- Consumer threads idle
- Queue not emptying

**Solution:**
1. Increase prefetch count
2. Enable batch acknowledgements
3. Optimize database queries
4. Check network latency to RabbitMQ

---

## Summary

### Key Takeaways

1. **Always align threads with database connections**
   - Formula: `(threads × pods) ≤ DB connections`

2. **One channel per thread is required**
   - Set: `spring.rabbitmq.cache.connection.mode: channel`

3. **Use transactions for atomicity**
   - Business logic + ACK in same transaction

4. **Enable batch ACK for performance**
   - Only if prefetch > 1

5. **Monitor queue depth and consumer count**
   - Set up alerts for anomalies

### Recommended Defaults

```yaml
# Safe for most deployments (up to 5 pods)
app:
  messaging:
    async:
      executor:
        core-threads: 5
        max-threads: 10
      processing:
        timeout-ms: 30000

  rabbitmq:
    listener:
      batch-size: 10
      enable-batch-ack: true

spring:
  rabbitmq:
    cache:
      connection:
        mode: channel
    listener:
      simple:
        concurrency: 5
        max-concurrency: 10
        prefetch: 50

  datasource:
    hikari:
      maximum-pool-size: 50
      minimum-idle: 10
```

---

**Document Version:** 1.0
**Last Updated:** 2024-01-XX
**Maintained By:** Platform Engineering Team
