package com.pharmacy;

import com.pharmacy.dto.CheckoutRequest;
import com.pharmacy.dto.CheckoutResponse;
import com.pharmacy.event.OrderEvent;
import com.pharmacy.producer.OrderProducerService;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderProducerTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private OrderProducerService orderProducerService;

    @BeforeEach
    void setUp() {
        orderProducerService = new OrderProducerService(kafkaTemplate);
    }

    @Test
    void testProcessCheckout_success_routesWithMedicineIdKey() {
        CheckoutRequest request = new CheckoutRequest("ORD-123", "MED-888", 10);

        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("medicine-stock-events", 2),
                0L, 0, 0L, 0, 0);
        SendResult<String, Object> sendResult = new SendResult<>(null, metadata);
        CompletableFuture<SendResult<String, Object>> future = CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(eq("medicine-stock-events"), eq("MED-888"), any(OrderEvent.class)))
                .thenReturn(future);

        CheckoutResponse response = orderProducerService.processCheckout(request);

        assertNotNull(response);
        assertEquals("SUCCESS", response.status());
        assertEquals("ORD-123", response.orderId());
        assertEquals("MED-888", response.medicineId());
        assertEquals(10, response.quantity());
        assertEquals(2, response.partition());

        verify(kafkaTemplate, times(1)).send(eq("medicine-stock-events"), eq("MED-888"), any(OrderEvent.class));
    }
}
