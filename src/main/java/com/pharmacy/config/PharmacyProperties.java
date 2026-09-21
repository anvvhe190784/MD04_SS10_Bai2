package com.pharmacy.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pharmacy")
public record PharmacyProperties(Kafka kafka) {
    public record Kafka(Topics topics) {}
    public record Topics(String stockEvents) {}
}
