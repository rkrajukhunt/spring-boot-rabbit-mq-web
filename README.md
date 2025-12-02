# RabbitMQ High-Throughput Messaging System

A Spring Boot 3.5.8 application that handles millions of messages using RabbitMQ with priority-based routing, exponential backoff retry mechanism, and request tracking in MSSQL.

## Architecture Overview

```
REST API (POST) → Publish to RabbitMQ → Consumer processes → Call createCommunication()
                      ↓                         ↓
                 Return TrackingID         Update Status in DB
```

### Key Features

- **High Throughput**: Handles 1M+ messages/day
- **Priority-based Routing**: 3 queues (HIGH, MEDIUM, LOW priority)
- **Retry Mechanism**: Exponential backoff (2s, 4s, 8s)
- **Request Tracking**: MSSQL database with status tracking
- **Concurrent Processing**: 10-20 consumers per queue (60 total)
- **Message Durability**: No message loss with manual ACK
- **Dead Letter Queue**: Failed messages after retries

## Prerequisites

- Java 21
- Docker & Docker Compose (for RabbitMQ and MSSQL)
- Maven 3.6+

## Quick Start

### 1. Start Infrastructure

```bash
cd c:\Users\RajuKhunt\Downloads\rabbitmq-demo\rabbit-mq-demo
docker-compose up -d
```

This starts:
- **RabbitMQ**: localhost:5672 (AMQP), localhost:15672 (Management UI)
- **MSSQL**: localhost:1433

**RabbitMQ Management UI**: http://localhost:15672
- Username: `admin`
- Password: `admin123`

### 2. Create Database

Connect to MSSQL and run:

```bash
docker exec -it mssql-server /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P YourStrong@Password123 -C
```

Then execute the script:

```sql
:r scripts/create-database.sql
GO
```

Or manually run the SQL in `scripts/create-database.sql`

### 3. Build Application

```bash
mvnw clean package
```

### 4. Run Application

```bash
mvnw spring-boot:run
```

Or:

```bash
java -jar target/demo-0.0.1-SNAPSHOT.jar
```

Application starts on: http://localhost:8080

## API Endpoints

### 1. Publish Message

**POST** `/api/v1/messages/publish`

**Request Body:**
```json
{
  "payload": "{\"userId\": \"12345\", \"message\": \"Hello World\"}",
  "priority": "HIGH",
  "metadata": {
    "source": "api",
    "version": "1.0"
  }
}
```

**Response (202 Accepted):**
```json
{
  "trackingId": "MSG-A1B2C3D4E5F6G7H8I9J0",
  "status": "RECEIVED",
  "message": "Message accepted for processing",
  "timestamp": "2025-12-02T10:30:00"
}
```

**Priority Values:**
- `HIGH` → Routes to `message-queue-1`
- `MEDIUM` → Routes to `message-queue-2`
- `LOW` → Routes to `message-queue-3`

### 2. Track Message

**GET** `/api/v1/messages/track/{trackingId}`

**Response (200 OK):**
```json
{
  "id": 1,
  "trackingId": "MSG-A1B2C3D4E5F6G7H8I9J0",
  "payload": "{\"userId\": \"12345\"}",
  "priority": "HIGH",
  "status": "COMPLETED",
  "queueName": "message-queue-1",
  "retryCount": 0,
  "errorMessage": null,
  "createdAt": "2025-12-02T10:30:00",
  "updatedAt": "2025-12-02T10:30:15",
  "completedAt": "2025-12-02T10:30:15"
}
```

**Status Values:**
- `RECEIVED` - Message accepted via API
- `PROCESSING` - Consumer is processing
- `COMPLETED` - Successfully processed
- `RETRY` - Sent to retry queue
- `FAILED` - Processing failed
- `DEAD_LETTER` - Moved to DLQ after retries

### 3. Get Statistics

**GET** `/api/v1/messages/stats`

**Response (200 OK):**
```json
{
  "total": 10000,
  "received": 50,
  "processing": 100,
  "completed": 9800,
  "failed": 0,
  "retry": 30,
  "dead_letter": 20
}
```

## Testing

### Test with cURL

```bash
# Publish HIGH priority message
curl -X POST http://localhost:8080/api/v1/messages/publish \
  -H "Content-Type: application/json" \
  -d '{
    "payload": "{\"test\": \"data\"}",
    "priority": "HIGH"
  }'

# Get tracking info
curl http://localhost:8080/api/v1/messages/track/MSG-ABC123...

# Get statistics
curl http://localhost:8080/api/v1/messages/stats
```

### Load Testing

Send 1000 messages:

```bash
for i in {1..1000}; do
  curl -X POST http://localhost:8080/api/v1/messages/publish \
    -H "Content-Type: application/json" \
    -d "{\"payload\": \"{\\\"test\\\": \\\"message-$i\\\"}\", \"priority\": \"HIGH\"}" &
done
```

## RabbitMQ Queue Structure

### Main Queues
- `message-queue-1` (HIGH priority)
- `message-queue-2` (MEDIUM priority)
- `message-queue-3` (LOW priority)

### Retry Queues (per main queue)
- `message-queue-1.retry.1` (TTL: 2000ms)
- `message-queue-1.retry.2` (TTL: 4000ms)
- `message-queue-1.retry.3` (TTL: 8000ms)

### Dead Letter Queues
- `message-queue-1.dlq`
- `message-queue-2.dlq`
- `message-queue-3.dlq`

### Retry Flow

```
Main Queue → Fails → Retry-1 (2s delay) → Main Queue
                  ↓
                Fails → Retry-2 (4s delay) → Main Queue
                  ↓
                Fails → Retry-3 (8s delay) → Main Queue
                  ↓
                Fails → DLQ (Dead Letter Queue)
```

## Performance

### Throughput Estimates

**Single Application Instance:**
- **60 consumers** (3 queues × 20 max threads)
- **Processing time 5s**: 720 msgs/min → 43,200/hour → **1M/day**
- **Processing time 1s**: 3,600 msgs/min → 216,000/hour → **5.2M/day**

**3 Application Instances:**
- **180 consumers**
- **Processing time 1s**: 10,800 msgs/min → 648,000/hour → **15.5M/day**

### Performance Tuning

**Increase Consumer Concurrency:**
```yaml
# In application.yaml
spring.rabbitmq.listener.simple.max-concurrency: 30  # From 20
```

**Increase Prefetch:**
```yaml
spring.rabbitmq.listener.simple.prefetch: 100  # From 50
```

**Optimize Business Logic:**
- Reduce `createCommunication()` processing time
- Add caching
- Use async operations
- Batch database operations

## Monitoring

### RabbitMQ Management UI

http://localhost:15672 (admin/admin123)

Monitor:
- Queue depths
- Message rates (published/delivered per second)
- Consumer count (should be 60 for single instance)
- Unacknowledged messages

### Database Queries

```sql
-- Messages by status
SELECT status, COUNT(*) as count
FROM message_tracking
GROUP BY status;

-- Messages in last hour
SELECT COUNT(*) as count
FROM message_tracking
WHERE created_at > DATEADD(hour, -1, GETDATE());

-- Average retry count
SELECT AVG(CAST(retry_count AS FLOAT)) as avg_retries
FROM message_tracking
WHERE retry_count > 0;

-- Messages in DLQ
SELECT *
FROM message_tracking
WHERE status = 'DEAD_LETTER'
ORDER BY created_at DESC;
```

### Application Logs

```
2025-12-02 10:30:00 [main] INFO  c.e.demo.config.RabbitMQConfig - RabbitMQ Listener Container Factory configured
2025-12-02 10:30:15 [http-nio-8080-exec-1] INFO  c.e.demo.controller.MessageController - Received message request
2025-12-02 10:30:15 [http-nio-8080-exec-1] INFO  c.e.demo.service.MessagePublisherService - Published message MSG-ABC123
2025-12-02 10:30:16 [SimpleAsyncTaskExecutor-1] INFO  c.e.demo.consumer.MessageConsumer - Processing message MSG-ABC123
2025-12-02 10:30:21 [SimpleAsyncTaskExecutor-1] INFO  c.e.demo.consumer.MessageConsumer - Successfully processed MSG-ABC123
```

## Configuration

### Key Configuration Files

**application.yaml:**
- RabbitMQ connection settings
- Consumer concurrency (10-20 per queue)
- Prefetch count (50)
- Database connection pool (50)
- Retry attempts (3)

**docker-compose.yml:**
- RabbitMQ and MSSQL containers
- Port mappings
- Volume persistence

## Troubleshooting

### Issue: Messages stuck in queue

**Check:**
1. Consumer threads running: Check logs for "Processing message"
2. Database connectivity: Test connection
3. Business logic errors: Check error_message in database

**Solution:**
- Increase concurrency
- Optimize business logic
- Check external dependencies

### Issue: High retry rate

**Check:**
1. Query database for error patterns
2. Review error_message field
3. Check external API availability

**Solution:**
- Fix underlying issue causing failures
- Adjust retry delays if needed
- Add circuit breaker for external calls

### Issue: DLQ accumulating messages

**Check:**
1. Query DLQ queues in RabbitMQ UI
2. Review messages in DLQ:

```sql
SELECT *
FROM message_tracking
WHERE status = 'DEAD_LETTER'
ORDER BY created_at DESC;
```

**Solution:**
- Investigate common failure patterns
- Fix root cause
- Consider manual reprocessing
- Set up alerting

### Issue: Slow processing

**Check:**
1. Profile `createCommunication()` method
2. Monitor database query performance
3. Check CPU/memory usage

**Solution:**
- Optimize database queries
- Add indexes
- Increase consumer threads
- Use connection pooling
- Cache frequently accessed data

## Production Checklist

- [ ] Change RabbitMQ credentials
- [ ] Change MSSQL password
- [ ] Enable SSL/TLS for RabbitMQ
- [ ] Add Spring Security for REST endpoints
- [ ] Use environment variables for sensitive config
- [ ] Enable database encryption
- [ ] Add API rate limiting
- [ ] Implement authentication/authorization
- [ ] Set up monitoring (Prometheus/Grafana)
- [ ] Configure alerting
- [ ] Set up log aggregation (ELK stack)
- [ ] Plan backup strategy
- [ ] Document operational procedures

## Project Structure

```
src/main/java/com/example/demo/
├── config/
│   └── RabbitMQConfig.java         # Queue/exchange/binding setup
├── controller/
│   └── MessageController.java      # REST endpoints
├── service/
│   ├── MessagePublisherService.java # Publishing logic
│   ├── TrackingService.java        # Database operations
│   └── CommunicationService.java   # Business logic
├── consumer/
│   └── MessageConsumer.java        # Message listeners + retry
├── model/
│   ├── dto/                         # Request/Response/Payload DTOs
│   └── entity/                      # JPA entities
├── enums/
│   ├── MessagePriority.java        # HIGH, MEDIUM, LOW
│   └── MessageStatus.java          # Status constants
├── repository/
│   └── MessageTrackingRepository.java
└── exception/
    ├── MessageProcessingException.java
    ├── ResourceNotFoundException.java
    └── GlobalExceptionHandler.java
```

## Contributing

1. Fork the repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request

## License

This project is licensed under the MIT License.

## Support

For issues and questions:
- Check troubleshooting section
- Review application logs
- Check RabbitMQ Management UI
- Query database for message status
