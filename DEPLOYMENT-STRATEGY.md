# Deployment Strategy: Separate Publisher and Consumer Pods

## Overview

You can deploy your RabbitMQ application in two configurations:
1. **Combined Mode** (default): Each pod has both publishers and consumers
2. **Separated Mode**: Dedicated publisher pods and dedicated consumer pods

---

## 🎯 **Strategy: Separate Publisher & Consumer Pods**

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      RabbitMQ Cluster                    │
│          inappcommunication.messages-fed (Queue)         │
└───────────────────┬────────────────┬────────────────────┘
                    │                │
        ┌───────────┴─────┐   ┌─────┴──────────┐
        │                 │   │                │
   ┌────▼────┐       ┌────▼────┐         ┌────▼────┐
   │Publisher│       │Consumer │         │Consumer │
   │  Pod 1  │       │  Pod 1  │         │  Pod 2  │
   │         │       │         │         │         │
   │ REST API│       │50-100   │         │50-100   │
   │Publishes│       │Consumers│         │Consumers│
   └─────────┘       └─────────┘         └─────────┘
```

### Benefits

✅ **Independent Scaling**
- Scale publishers based on API traffic
- Scale consumers based on queue depth

✅ **Resource Optimization**
- Publisher pods: CPU for REST API, minimal memory
- Consumer pods: CPU/memory for message processing

✅ **Better Isolation**
- Publisher issues don't affect consumers
- Consumer slowness doesn't affect API response time

✅ **Flexible Deployment**
- Deploy more consumer pods during high load
- Deploy more publisher pods during peak API traffic

---

## 📝 **Implementation Approaches**

### Approach 1: Spring Profiles (Recommended)

**Create two Spring profiles:**
- `publisher` - Enables REST API, disables consumers
- `consumer` - Enables consumers, disables REST API

#### Step 1: Create Profile-Specific Configuration

**application-publisher.yaml:**
```yaml
spring:
  profiles:
    active: publisher

  # RabbitMQ configuration for publishers
  rabbitmq:
    host: ${RABBITMQ_HOST}
    port: 5671
    username: ${RABBITMQ_SERVICE_ACCOUNT_USER}
    password: ${RABBITMQ_SERVICE_ACCOUNT_PASSWORD}

    cache:
      connection:
        size: 1
      channel:
        size: 20  # Fewer channels needed for publishers

    publisher-confirm-type: correlated
    publisher-returns: true

    # Disable listener container (no consumers)
    listener:
      simple:
        auto-startup: false  # KEY: Disable consumers!

# Enable REST API
server:
  port: 8080

# Actuator for monitoring
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus

app:
  # Publisher-specific config
  mode: publisher
```

**application-consumer.yaml:**
```yaml
spring:
  profiles:
    active: consumer

  # RabbitMQ configuration for consumers
  rabbitmq:
    host: ${RABBITMQ_HOST}
    port: 5671
    username: ${RABBITMQ_SERVICE_ACCOUNT_USER}
    password: ${RABBITMQ_SERVICE_ACCOUNT_PASSWORD}

    cache:
      connection:
        size: 1
      channel:
        size: 120  # Full channel pool for consumers

    # Consumer configuration
    listener:
      simple:
        auto-startup: true  # Enable consumers
        concurrency: 50
        max-concurrency: 100
        prefetch: 50
        acknowledge-mode: manual

# Disable REST API (optional - or keep for health checks)
server:
  port: 8081  # Different port, or keep 8080 for health checks

# Actuator for monitoring
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus,rabbitmq

app:
  # Consumer-specific config
  mode: consumer
```

#### Step 2: Conditional Bean Configuration

**Update MessageConsumer.java:**
```java
@Component
@Slf4j
@RequiredArgsConstructor
@Profile("!publisher")  // Only active when NOT publisher profile
public class MessageConsumer {
    // Existing consumer code
}
```

**Update REST Controller:**
```java
@RestController
@RequestMapping("/api/v1/messages")
@RequiredArgsConstructor
@Slf4j
@Profile("!consumer")  // Only active when NOT consumer profile
public class MessageController {
    // Existing REST API code
}
```

#### Step 3: Kubernetes Deployments

**publisher-deployment.yaml:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rabbitmq-publisher
  namespace: inappcommunication
spec:
  replicas: 2  # Scale based on API traffic
  selector:
    matchLabels:
      app: rabbitmq-app
      role: publisher
  template:
    metadata:
      labels:
        app: rabbitmq-app
        role: publisher
    spec:
      containers:
      - name: publisher
        image: your-registry/rabbitmq-app:latest
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: "prod,publisher"  # Activate publisher profile
        - name: RABBITMQ_HOST
          valueFrom:
            configMapKeyRef:
              name: rabbitmq-config
              key: RABBITMQ_HOST
        - name: RABBITMQ_SERVICE_ACCOUNT_USER
          valueFrom:
            secretKeyRef:
              name: rabbitmq-credentials
              key: username
        - name: RABBITMQ_SERVICE_ACCOUNT_PASSWORD
          valueFrom:
            secretKeyRef:
              name: rabbitmq-credentials
              key: password
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        ports:
        - containerPort: 8080
          name: http
        livenessProbe:
          httpGet:
            path: /actuator/health
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: rabbitmq-publisher
  namespace: inappcommunication
spec:
  selector:
    app: rabbitmq-app
    role: publisher
  ports:
  - port: 80
    targetPort: 8080
    name: http
  type: ClusterIP
```

**consumer-deployment.yaml:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rabbitmq-consumer
  namespace: inappcommunication
spec:
  replicas: 3  # Scale based on queue depth
  selector:
    matchLabels:
      app: rabbitmq-app
      role: consumer
  template:
    metadata:
      labels:
        app: rabbitmq-app
        role: consumer
    spec:
      containers:
      - name: consumer
        image: your-registry/rabbitmq-app:latest
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: "prod,consumer"  # Activate consumer profile
        - name: RABBITMQ_HOST
          valueFrom:
            configMapKeyRef:
              name: rabbitmq-config
              key: RABBITMQ_HOST
        - name: RABBITMQ_SERVICE_ACCOUNT_USER
          valueFrom:
            secretKeyRef:
              name: rabbitmq-credentials
              key: username
        - name: RABBITMQ_SERVICE_ACCOUNT_PASSWORD
          valueFrom:
            secretKeyRef:
              name: rabbitmq-credentials
              key: password
        resources:
          requests:
            memory: "1Gi"      # More memory for message processing
            cpu: "1000m"        # More CPU for 50-100 consumers
          limits:
            memory: "2Gi"
            cpu: "2000m"
        ports:
        - containerPort: 8081
          name: metrics
        livenessProbe:
          httpGet:
            path: /actuator/health
            port: 8081
          initialDelaySeconds: 60
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8081
          initialDelaySeconds: 30
          periodSeconds: 5
```

---

### Approach 2: Separate Application Builds (Alternative)

Create two separate Spring Boot applications:

**1. rabbitmq-publisher (API only)**
- Contains REST controllers
- MessagePublisherService
- No @RabbitListener components

**2. rabbitmq-consumer (Consumer only)**
- Contains MessageConsumer with @RabbitListener
- No REST controllers
- Processing services

---

## 🔧 **Scaling Configuration**

### Horizontal Pod Autoscaler (HPA)

**Publisher HPA (based on HTTP requests):**
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: rabbitmq-publisher-hpa
  namespace: inappcommunication
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: rabbitmq-publisher
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Pods
    pods:
      metric:
        name: http_requests_per_second
      target:
        type: AverageValue
        averageValue: "1000"
```

**Consumer HPA (based on queue depth):**
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: rabbitmq-consumer-hpa
  namespace: inappcommunication
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: rabbitmq-consumer
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: External
    external:
      metric:
        name: rabbitmq_queue_messages
        selector:
          matchLabels:
            queue: inappcommunication.messages-fed
      target:
        type: AverageValue
        averageValue: "1000"  # Scale up if queue > 1000 msgs per pod
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300  # Wait 5 min before scaling down
    scaleUp:
      stabilizationWindowSeconds: 60   # Scale up quickly
```

---

## 📊 **Resource Allocation**

### Publisher Pods
```yaml
resources:
  requests:
    memory: "512Mi"   # Lightweight, just REST API
    cpu: "500m"
  limits:
    memory: "1Gi"
    cpu: "1000m"
```

**Why:**
- Minimal memory for API processing
- CPU for handling HTTP requests
- No heavy message processing

### Consumer Pods
```yaml
resources:
  requests:
    memory: "1Gi"     # Message processing + DB operations
    cpu: "1000m"      # 50-100 consumer threads
  limits:
    memory: "2Gi"
    cpu: "2000m"
```

**Why:**
- More memory for processing messages and DB operations
- More CPU for 50-100 concurrent consumer threads

---

## 🎯 **Capacity Planning**

### Example Setup

**Scenario: 10M messages/day, 1s processing time**

**Publisher Pods:**
- 2-5 pods (based on API traffic)
- Each can publish 1000+ msgs/sec
- Total capacity: 2,000-5,000 msgs/sec

**Consumer Pods:**
- 3 pods × 100 consumers = 300 consumers
- Each processes 1 msg/sec
- Total capacity: 300 msgs/sec = 25.9M/day

**Result:** ✅ Consumers handle 10M/day easily

---

## 🔍 **Monitoring & Alerting**

### Publisher Metrics
- HTTP request rate
- Message publish rate
- Publisher confirm failures
- API response time

### Consumer Metrics
- Queue depth
- Consumer count (should be 50-100 per pod)
- Message processing time
- Error rate / DLQ count

### Grafana Queries
```promql
# Queue depth
rabbitmq_queue_messages{queue="inappcommunication.messages-fed"}

# Consumer count
rabbitmq_queue_consumers{queue="inappcommunication.messages-fed"}

# Messages per second (published)
rate(rabbitmq_queue_messages_published_total[1m])

# Messages per second (consumed)
rate(rabbitmq_queue_messages_acked_total[1m])
```

---

## ⚙️ **Configuration Summary**

### Current (Combined Mode)
```
Pods: 3
- Each has REST API + 50-100 consumers
- Total: 150-300 consumers
- Good for: Simple deployments
```

### Recommended (Separated Mode)
```
Publisher Pods: 2-5
- REST API only
- Scales with API traffic

Consumer Pods: 3-20
- Consumers only (50-100 each)
- Total: 150-2000 consumers
- Scales with queue depth
- Good for: Production, better resource control
```

---

## 🚀 **Deployment Steps**

### 1. Create Profile-Specific Configs
```bash
# Create application-publisher.yaml
# Create application-consumer.yaml
```

### 2. Update Code with @Profile Annotations
```java
@Component
@Profile("!publisher")
public class MessageConsumer { ... }

@RestController
@Profile("!consumer")
public class MessageController { ... }
```

### 3. Build Docker Image (same image for both)
```dockerfile
FROM openjdk:21-jdk-slim
COPY target/demo-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
```

### 4. Deploy to Kubernetes
```bash
kubectl apply -f publisher-deployment.yaml
kubectl apply -f consumer-deployment.yaml
```

### 5. Verify
```bash
# Check publisher pods
kubectl get pods -l role=publisher

# Check consumer pods
kubectl get pods -l role=consumer

# Check consumer count in RabbitMQ
curl http://<consumer-pod>/actuator/rabbitmq/consumer-count
```

---

## 🎉 **Summary**

| Aspect | Combined Mode | Separated Mode |
|--------|---------------|----------------|
| **Deployment** | Simple | More complex |
| **Scaling** | Scale everything | Scale independently |
| **Resources** | Less efficient | More efficient |
| **Isolation** | Low | High |
| **Best For** | Dev/QA | Production |

**Recommendation:** Use **Separated Mode** for production with Spring Profiles approach for flexibility!
