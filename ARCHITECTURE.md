# RabbitMQ High-Throughput Architecture - 6 Queues with Load Balancing

## Architecture Overview

### Queue Structure (6 Queues: 2 per Priority Level)

```
HIGH PRIORITY (2 queues):
- message-queue-high-1  (10-20 consumers)
- message-queue-high-2  (10-20 consumers)

MEDIUM PRIORITY (2 queues):
- message-queue-medium-1  (10-20 consumers)
- message-queue-medium-2  (10-20 consumers)

LOW PRIORITY (2 queues):
- message-queue-low-1  (10-20 consumers)
- message-queue-low-2  (10-20 consumers)

Total: 120 consumers (6 queues × 20 max threads)
```

### Message Flow

```
┌────────────────────────────────────────────────────────────────┐
│  1. POST /api/v1/messages/publish                             │
│     {                                                          │
│       "payload": "{...}",                                      │
│       "priority": "HIGH"  // Optional (defaults to MEDIUM)    │
│     }                                                          │
└──────────────────────┬─────────────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────────────┐
│  2. Auto-assign Priority (if not provided)                     │
│     Default: MEDIUM                                            │
└──────────────────────┬─────────────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────────────┐
│  3. Load Balancer Selects Best Queue                          │
│     Strategy:                                                  │
│     - Check queue depths (message count)                       │
│     - Select queue with lowest depth                           │
│     - Fallback: Round-robin if depth unavailable              │
└──────────────────────┬─────────────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────────────┐
│  4. Publish to RabbitMQ Exchange                              │
│     Exchange: "message.exchange" (Topic)                       │
│     Routing Keys:                                              │
│     - message.priority.high → high-1 & high-2                 │
│     - message.priority.medium → medium-1 & medium-2           │
│     - message.priority.low → low-1 & low-2                    │
└──────────────────────┬─────────────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────────────┐
│  5. Consumer Processes Message                                 │
│     - Update DB status: PROCESSING                             │
│     - Call createCommunication()                               │
│     - Update DB status: COMPLETED                              │
│     - ACK message                                              │
└──────────────────────┬─────────────────────────────────────────┘
                       │
                  ┌────┴────┐
                  │Success? │
                  └────┬────┘
                       │
          ┌────────────┴────────────┐
          │ YES                     │ NO
          ▼                         ▼
    ┌─────────┐          ┌─────────────────────┐
    │ DONE    │          │ Retry Mechanism     │
    └─────────┘          └──────────┬──────────┘
                                    │
                         ┌──────────┴──────────┐
                         │ Retry Count < 3?    │
                         └──────────┬──────────┘
                                    │
                       ┌────────────┴────────────┐
                       │ YES                     │ NO
                       ▼                         ▼
            ┌─────────────────────┐    ┌──────────────┐
            │ Send to Retry Queue │    │ Send to DLQ  │
            │ - retry.1 (2s TTL)  │    │ Status: DEAD │
            │ - retry.2 (4s TTL)  │    │     _LETTER  │
            │ - retry.3 (8s TTL)  │    └──────────────┘
            │ Then back to main   │
            └─────────────────────┘
```

---

## Key Features

### 1. Automatic Priority Assignment

**Feature:** Priority is now **optional** in API requests.

**Default Behavior:**
- If `priority` field is `null` or not provided → Auto-assigned to `MEDIUM`
- Logs: `"Priority not provided for message {id}, auto-assigned to MEDIUM"`

**Example Requests:**

```bash
# With priority (explicit)
{
  "payload": "{\"test\": \"data\"}",
  "priority": "HIGH"
}

# Without priority (auto-assigned to MEDIUM)
{
  "payload": "{\"test\": \"data\"}"
}
```

---

### 2. Intelligent Load Balancing

**LoadBalancerService** automatically selects the best queue:

**Strategy:**
1. **Primary:** Check queue depths via RabbitAdmin
   - Query both queues for a priority level
   - Select queue with **lowest message count**
   - Example: HIGH priority → check `high-1` (500 msgs) vs `high-2` (200 msgs) → select `high-2`

2. **Fallback:** Round-robin if queue info unavailable
   - Maintains separate counters for HIGH, MEDIUM, LOW
   - Alternates: HIGH → high-1 → high-2 → high-1...

**Benefits:**
- **Even distribution** across queues
- **Prevents hotspots** (one queue overloaded)
- **Better throughput** by utilizing all available consumers

---

### 3. Efficient Consumer Architecture

**6 `@RabbitListener` Methods:**
- Each queue has its own listener method
- **Shared processing logic** via `processMessage()` method
- **No code duplication** for retry/error handling

**Consumer Count:**
- 6 queues × 10-20 threads = **60-120 total consumers**
- Each consumer prefetches 50 messages
- **Maximum in-flight messages:** 120 consumers × 50 = **6,000 messages**

**Efficiency:**
- **Single JVM** handles all 6 queues
- **Thread pool reuse** via Spring's container factory
- **Manual ACK** ensures no message loss

---

## Performance Characteristics

### Throughput Estimates

**Single Application Instance:**
- **120 consumers** (6 queues × 20 max threads)
- **Processing time 5s:** 1,440 msgs/min → 86,400/hour → **2.07M/day**
- **Processing time 1s:** 7,200 msgs/min → 432,000/hour → **10.4M/day**

**3 Application Instances (Horizontal Scaling):**
- **360 total consumers**
- **Processing time 1s:** 21,600 msgs/min → 1.3M/hour → **31M/day**

**Comparison to 3-Queue Setup:**
- **Before:** 3 queues × 20 threads = 60 consumers → **5.2M/day** (1s processing)
- **After:** 6 queues × 20 threads = 120 consumers → **10.4M/day** (1s processing)
- **Improvement:** **2x throughput** 🚀

---

## Queue Distribution Logic

### Load Balancing Example

**Scenario:** 1000 messages with HIGH priority arrive

**Without Load Balancing (all to queue-1):**
```
message-queue-high-1: 1000 messages (overloaded)
message-queue-high-2: 0 messages (idle)
```

**With Load Balancing:**
```
Round-robin:
message-queue-high-1: 500 messages
message-queue-high-2: 500 messages

Queue-depth based:
message-queue-high-1: 450 messages
message-queue-high-2: 550 messages
(Dynamically adjusts based on processing speed)
```

**Result:** Both queues utilized, faster overall processing

---

## Configuration

### application.yaml

No changes needed from original configuration. The system automatically handles 6 queues.

```yaml
spring:
  rabbitmq:
    listener:
      simple:
        concurrency: 10
        max-concurrency: 20
        prefetch: 50
```

---

## Retry Mechanism (Per Queue)

Each of the 6 main queues has its own retry chain:

**Example for `message-queue-high-1`:**

```
Main: message-queue-high-1
  ├─ Retry 1: message-queue-high-1.retry.1 (TTL: 2000ms)
  ├─ Retry 2: message-queue-high-1.retry.2 (TTL: 4000ms)
  ├─ Retry 3: message-queue-high-1.retry.3 (TTL: 8000ms)
  └─ DLQ: message-queue-high-1.dlq

Total retry queues: 6 main × 3 retries = 18 retry queues
Total DLQ queues: 6
```

**After 3 failed retries:**
- Message moves to respective DLQ
- Status in DB: `DEAD_LETTER`
- DLQ listener logs the failure for monitoring

---

## Monitoring

### RabbitMQ Management UI

**Queues to Monitor:**

**Main Queues (6):**
- message-queue-high-1, message-queue-high-2
- message-queue-medium-1, message-queue-medium-2
- message-queue-low-1, message-queue-low-2

**Metrics per queue:**
- Message count (ready)
- Consumer count (should be 10-20 per queue)
- Message rate (published/delivered per second)

**Total Expected Consumers:** 120 (6 × 20)

### Load Balancer Effectiveness

**Check distribution:**
```bash
# In RabbitMQ UI, compare message counts
High-1: 1,234 messages, 20 consumers
High-2: 1,198 messages, 20 consumers  ✓ Well balanced!

Medium-1: 5,000 messages, 20 consumers
Medium-2: 50 messages, 15 consumers   ✗ Imbalanced (investigate)
```

**Reasons for imbalance:**
- Some consumers may have died
- Processing time varies
- Burst of messages to one queue

**Solution:** Load balancer will auto-correct on next publish

---

## API Changes

### Request Schema

**Before (priority required):**
```json
{
  "payload": "{...}",
  "priority": "HIGH"  // REQUIRED
}
```

**After (priority optional):**
```json
{
  "payload": "{...}",
  "priority": "HIGH"  // OPTIONAL (defaults to MEDIUM)
}
```

### Response Schema

**No changes** - still returns tracking ID immediately:
```json
{
  "trackingId": "MSG-ABC123...",
  "status": "RECEIVED",
  "message": "Message accepted for processing",
  "timestamp": "2025-12-02T10:30:00"
}
```

---

## Database Schema

**No changes** to `message_tracking` table. The `queue_name` field now stores values like:
- `message-queue-high-1`
- `message-queue-medium-2`
- etc.

**Query to see distribution:**
```sql
SELECT
    queue_name,
    COUNT(*) as message_count
FROM message_tracking
WHERE status = 'PROCESSING'
GROUP BY queue_name
ORDER BY message_count DESC;
```

**Expected result:**
```
queue_name              message_count
message-queue-high-1    150
message-queue-high-2    145
message-queue-medium-1  200
message-queue-medium-2  195
message-queue-low-1     50
message-queue-low-2     48
```

---

## Testing Load Balancing

### Test Round-Robin

```bash
# Send 10 messages without priority (should alternate)
for i in {1..10}; do
  curl -X POST http://localhost:8080/api/v1/messages/publish \
    -H "Content-Type: application/json" \
    -d "{\"payload\": \"{\\\"test\\\": \\\"message-$i\\\"}\"}"
done
```

**Check logs:** Should see messages distributed across `medium-1` and `medium-2`

### Test Queue Depth Selection

```bash
# Overload medium-1 first
for i in {1..1000}; do
  curl -X POST http://localhost:8080/api/v1/messages/publish \
    -H "Content-Type: application/json" \
    -d '{"payload": "{\"test\": \"data\"}", "priority": "MEDIUM"}' &
done

# Wait 5 seconds, then send more
sleep 5

# Load balancer should now favor medium-2
for i in {1001..2000}; do
  curl -X POST http://localhost:8080/api/v1/messages/publish \
    -H "Content-Type: application/json" \
    -d '{"payload": "{\"test\": \"data\"}", "priority": "MEDIUM"}' &
done
```

**Check RabbitMQ UI:** Queue depths should be more balanced

---

## Advantages of 6-Queue Architecture

### 1. Higher Throughput
- **2x consumers** (60 → 120)
- **2x processing capacity**

### 2. Better Load Distribution
- Messages spread across more queues
- Reduces queue depth per queue
- Faster message delivery

### 3. Improved Resilience
- If one queue has issues, others continue processing
- Better fault isolation

### 4. Scalability
- Can easily add more queues if needed (e.g., 3 per priority = 9 total)
- Linear scaling: more queues = more throughput

### 5. Fine-grained Control
- Monitor each queue independently
- Adjust consumer count per queue if needed
- Better observability

---

## Migration from 3-Queue to 6-Queue

**No breaking changes!** Existing code continues to work:

1. ✅ Old messages with `priority: HIGH` still route correctly
2. ✅ API contracts unchanged
3. ✅ Database schema unchanged
4. ✅ Retry mechanism same behavior

**What changed:**
- More queues created automatically on startup
- Load balancer selects between 2 queues per priority
- Logging shows selected queue name

---

## Summary

| Metric | 3-Queue Setup | 6-Queue Setup | Improvement |
|--------|---------------|---------------|-------------|
| **Main Queues** | 3 | 6 | +100% |
| **Total Consumers** | 60 | 120 | +100% |
| **Throughput (1s)** | 5.2M/day | 10.4M/day | +100% |
| **Priority Levels** | 3 | 3 | Same |
| **Load Balancing** | None | Intelligent | ✓ |
| **Auto-priority** | ✗ Required | ✓ Optional | ✓ |
| **Retry Queues** | 9 | 18 | +100% |
| **DLQ Queues** | 3 | 6 | +100% |

**Result:** System can now handle **10M+ messages per day** with intelligent load distribution! 🎉
