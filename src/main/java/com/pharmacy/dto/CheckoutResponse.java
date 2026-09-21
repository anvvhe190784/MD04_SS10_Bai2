package com.pharmacy.dto;

import java.time.Instant;

public record CheckoutResponse(
        String status,
        String message,
        String orderId,
        String medicineId,
        int quantity,
        String topic,
        int partition,
        long offset,
        Instant timestamp) {
    public static CheckoutResponse success(
            String orderId,
            String medicineId,
            int quantity,
            String topic,
            int partition,
            long offset) {
        return new CheckoutResponse(
                "SUCCESS",
                "Payment processed and event published successfully",
                orderId,
                medicineId,
                quantity,
                topic,
                partition,
                offset,
                Instant.now());
    }
}
