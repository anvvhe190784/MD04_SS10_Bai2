package com.pharmacy.event;

import java.time.Instant;

public record OrderEvent(
        String orderId,
        String medicineId,
        int quantity,
        Instant timestamp) {
    public static OrderEvent of(String orderId, String medicineId, int quantity) {
        return new OrderEvent(orderId, medicineId, quantity, Instant.now());
    }
}
