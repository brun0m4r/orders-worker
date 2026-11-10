package com.example.orders.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/** Documento guardado en MongoDB (colección "orders"). */
@Document("orders")
@CompoundIndex(name = "customer_created_idx", def = "{'customerId': 1, 'processedAt': -1}")
public record ProcessedOrder(
        @Id String id,
        @Indexed(unique = true) String orderId,
        String customerId,
        String customerName,
        List<Line> products,
        BigDecimal total,
        Instant processedAt) {

    public record Line(
            String productId,
            String name,
            String description,
            BigDecimal price,
            int quantity) {}
}
