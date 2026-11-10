package com.example.orders.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/** Mensaje que llega por Kafka: datos básicos del pedido. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderMessage(String orderId, String customerId, List<Item> products) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Item(String productId, Integer quantity) {
        public int quantityOrDefault() {
            return quantity == null ? 1 : quantity;
        }
    }
}
