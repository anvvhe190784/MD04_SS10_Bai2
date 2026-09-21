# Pharmacy Service - Kafka Order Event Producer (Exercise 2)

This repository contains the implementation for **[Bài tập khá 2] Triển khai Producer gửi sự kiện Đơn hàng thuốc** using **Spring Boot 4.1.1**, **Java 21**, **Gradle**, and **Apache Kafka 4.3.1**.

## 1. Overview & Architecture

When a pharmacy staff member initiates checkout, the `pharmacy-service` packages the transaction into an `OrderEvent` and publishes it to the Kafka topic `medicine-stock-events`.

- **Framework**: Spring Boot 4.1.1 (Java 21)
- **Build Tool**: Gradle (using Gradle Wrapper `gradlew`)
- **Message Broker**: Apache Kafka 4.3.1 (KRaft mode)
- **Topic**: `medicine-stock-events` (3 Partitions)
- **Message Key**: `medicineId`
  - *Guarantee*: Orders for the same medication are consistently routed to the same partition, preserving per-medicine event ordering.
- **Payload Format**: JSON (`JsonSerializer`)

## 2. Event Model (`OrderEvent`)

```java
public record OrderEvent(
    String orderId,
    String medicineId,
    int quantity,
    Instant timestamp
) {}
```

## 3. Getting Started

### Step 1: Start Kafka Broker (KRaft Mode)
Run the dedicated Kafka broker container:
```bash
docker compose up -d
```

Verify the broker and check topics:
```bash
docker ps --filter "name=kafka-pharmacy"
docker exec -i kafka-pharmacy /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

*(If `medicine-stock-events` does not exist yet, create it with 3 partitions)*:
```bash
docker exec -i kafka-pharmacy /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic medicine-stock-events \
  --partitions 3 \
  --replication-factor 1
```

### Step 2: Build & Run Tests
Execute the test suite using the Gradle wrapper:
```powershell
# Windows
.\gradlew.bat test

# Linux / macOS
./gradlew test
```

### Step 3: Run the Service
Start the Spring Boot application:
```powershell
# Windows
.\gradlew.bat bootRun

# Linux / macOS
./gradlew bootRun
```

The service runs at `http://localhost:8081`.

## 4. Testing the Checkout API

### Send Checkout Requests
Send sample checkout requests for the same medicine (`MED-101`) and a different medicine (`MED-202`):

```bash
# Order 1 for MED-101
curl -X POST http://localhost:8081/api/orders/checkout \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORD-001", "medicineId": "MED-101", "quantity": 5}'

# Order 2 for MED-101 (routes to the exact same partition)
curl -X POST http://localhost:8081/api/orders/checkout \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORD-002", "medicineId": "MED-101", "quantity": 2}'

# Order 3 for MED-202
curl -X POST http://localhost:8081/api/orders/checkout \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORD-003", "medicineId": "MED-202", "quantity": 10}'
```

### Response Example
```json
{
  "status": "SUCCESS",
  "message": "Payment processed and event published successfully",
  "orderId": "ORD-001",
  "medicineId": "MED-101",
  "quantity": 5,
  "topic": "medicine-stock-events",
  "partition": 1,
  "offset": 0,
  "timestamp": "2026-09-21T13:30:00Z"
}
```

## 5. Verifying Message Routing in Kafka

Run the Kafka console consumer inside the container to inspect message keys, partitions, and JSON payloads:

```bash
docker exec -i kafka-pharmacy /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic medicine-stock-events \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true
```

