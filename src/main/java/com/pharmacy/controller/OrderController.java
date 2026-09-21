package com.pharmacy.controller;

import com.pharmacy.dto.CheckoutRequest;
import com.pharmacy.dto.CheckoutResponse;
import com.pharmacy.producer.OrderProducerService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducerService orderProducerService;

    public OrderController(OrderProducerService orderProducerService) {
        this.orderProducerService = orderProducerService;
    }

    @PostMapping("/checkout")
    public ResponseEntity<CheckoutResponse> checkout(@Valid @RequestBody CheckoutRequest request) {
        CheckoutResponse response = orderProducerService.processCheckout(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}
