package com.example.orders.failure;

import com.example.orders.model.OrderMessage;
import java.time.Instant;

/** Mensaje fallido guardado en Redis junto con su contador de intentos. */
public record FailedOrder(
        OrderMessage message,
        int attempts,
        Status status,
        String lastError,
        Instant nextRetryAt,
        Instant failedAt) {

    public enum Status {
        /** Pendiente de reintento. */
        RETRYING,
        /** Agotó los reintentos. */
        DEAD,
        /** Rechazado por validación de negocio (no se reintenta). */
        REJECTED
    }

    public String orderId() {
        return message.orderId();
    }
}
