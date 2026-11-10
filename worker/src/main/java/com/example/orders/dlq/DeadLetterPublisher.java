package com.example.orders.dlq;

import com.example.orders.model.OrderMessage;
import reactor.core.publisher.Mono;

/**
 * Última red de seguridad: publica en un topic aparte (DLQ) los mensajes que no se pudieron
 * procesar ni registrar como fallidos, para que no se pierdan. Si publicar también falla,
 * el Mono emite error y el consumidor NO confirma el offset.
 */
public interface DeadLetterPublisher {

    /** Publica el payload tal cual llegó (por ejemplo, un JSON ilegible). */
    Mono<Void> publishRaw(String key, String payload, String reason);

    /** Publica un pedido ya parseado, serializado de nuevo a JSON. */
    Mono<Void> publish(OrderMessage message, String reason);
}
