package com.pharmacy.producer;

import com.pharmacy.dto.CheckoutRequest;
import com.pharmacy.dto.CheckoutResponse;
import com.pharmacy.event.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class OrderProducerService {

    private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);
    private static final String TOPIC_NAME = "medicine-stock-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CheckoutResponse processCheckout(CheckoutRequest request) {
        OrderEvent event = OrderEvent.of(
                request.orderId(),
                request.medicineId(),
                request.quantity());

        // Challenge requirement: Use medicineId as the Kafka Message Key to guarantee
        // identical medicines route to the same partition
        String messageKey = request.medicineId();

        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(TOPIC_NAME, messageKey, event);

            SendResult<String, Object> sendResult = future.get(5, TimeUnit.SECONDS);
            int partition = sendResult.getRecordMetadata().partition();
            long offset = sendResult.getRecordMetadata().offset();

            log.info(
                    "OrderEvent published successfully: orderId={}, medicineId={}, key={}, topic={}, partition={}, offset={}",
                    event.orderId(), event.medicineId(), messageKey, TOPIC_NAME, partition, offset);

            return CheckoutResponse.success(
                    event.orderId(),
                    event.medicineId(),
                    event.quantity(),
                    TOPIC_NAME,
                    partition,
                    offset);
        } catch (Exception e) {
            log.error("Failed to publish OrderEvent: orderId={}, medicineId={}", request.orderId(),
                    request.medicineId(), e);
            throw new RuntimeException("Error publishing order event to Kafka: " + e.getMessage(), e);
        }
    }
}
