package com.example.orders.service;

import com.example.orders.dlq.DeadLetterPublisher;
import com.example.orders.failure.FailedOrderStore;
import com.example.orders.failure.FailureService;
import com.example.orders.lock.DistributedLock;
import com.example.orders.lock.LockNotAcquiredException;
import com.example.orders.model.OrderMessage;
import com.example.orders.model.OrderValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Punto único de entrada para procesar un pedido (desde Kafka o desde el reintento):
 * lock distribuido -> proceso -> éxito limpia Redis; fallo se registra en Redis.
 * Los errores de un pedido se resuelven aquí (rechazo, reintento en Redis o DLQ), por lo que
 * normalmente completa sin error y el consumidor puede confirmar el offset. Solo emite error
 * si ni Redis ni el DLQ pudieron guardar el pedido: en ese caso NO se debe confirmar el offset.
 */
@Service
public class OrderHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderHandler.class);

    private final OrderProcessor processor;
    private final DistributedLock lock;
    private final FailureService failures;
    private final FailedOrderStore store;
    private final DeadLetterPublisher deadLetters;

    public OrderHandler(OrderProcessor processor, DistributedLock lock,
                        FailureService failures, FailedOrderStore store,
                        DeadLetterPublisher deadLetters) {
        this.processor = processor;
        this.lock = lock;
        this.failures = failures;
        this.store = store;
        this.deadLetters = deadLetters;
    }

    public Mono<Void> handle(OrderMessage msg, int previousAttempts) {
        String id = msg.orderId() == null ? "unknown" : msg.orderId();
        return lock.withLock(id, () -> processor.process(msg)
                        .doOnNext(o -> log.info("Order {} processed", o.orderId()))
                        .then(store.remove(id)))
                .onErrorResume(LockNotAcquiredException.class, e -> {
                    log.info("Order {} is being processed by another instance, skipping", id);
                    return Mono.empty();
                })
                .onErrorResume(OrderValidationException.class, e -> failures.reject(msg, e))
                .onErrorResume(e -> failures.recordFailure(msg, e, previousAttempts))
                // Redis no pudo registrar el fallo: última red de seguridad, el mensaje va al DLQ.
                // Si el DLQ también falla, el error se propaga y el consumidor no confirma el offset.
                .onErrorResume(e -> {
                    log.error("Could not record failure for order {}, sending to DLQ: {}", id, e.toString());
                    return deadLetters.publish(msg, "failure-store-unavailable: " + e);
                });
    }
}
