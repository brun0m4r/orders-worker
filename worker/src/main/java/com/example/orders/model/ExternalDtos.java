package com.example.orders.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;

/** Respuestas de la API de enriquecimiento (Go). */
public final class ExternalDtos {
    private ExternalDtos() {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Product(String id, String name, String description, BigDecimal price, String category) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Customer(String id, String name, String email, boolean active) {}
}
