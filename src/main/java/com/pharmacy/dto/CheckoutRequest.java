package com.pharmacy.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CheckoutRequest(
        @NotBlank(message = "orderId is required") String orderId,

        @NotBlank(message = "medicineId is required") String medicineId,

        @NotNull(message = "quantity is required") @Min(value = 1, message = "quantity must be greater than 0") Integer quantity) {
}
