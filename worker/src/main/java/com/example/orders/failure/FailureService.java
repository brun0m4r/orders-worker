package com.example.orders.failure;

import com.example.orders.config.WorkerProperties;
import com.example.orders.model.OrderMessage;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/** Decide qué hacer con un pedido que falló: reintentar con backoff exponencial, marcarlo DEAD o REJECTED. */
@Service
public class FailureService {

    private static final Logger log = LoggerFactory.getLogger(FailureService.class);

    private final FailedOrderStore store;
    private final WorkerProperties.Retry cfg;
    private final Clock clock;

    public FailureService(FailedOrderStore store, WorkerProperties props, Clock clock) {
        this.store = store;
        this.cfg = props.retry();
        this.clock = clock;
    }

    /** Falla transitoria: incrementa el contador y agenda el próximo intento, o pasa a DEAD si se agotaron. */
    public Mono<Void> recordFailure(OrderMessage msg, Throwable error, int previousAttempts) {
        int attempts = previousAttempts + 1;
        Instant now = clock.instant();
        if (attempts >= cfg.maxAttempts()) {
            log.error("Order {} exhausted {} attempts, marking DEAD: {}", msg.orderId(), attempts, error.toString());
            return store.save(new FailedOrder(msg, attempts, FailedOrder.Status.DEAD, describe(error), null, now));
        }
        Instant next = now.plus(backoff(attempts));
        log.warn("Order {} failed (attempt {}/{}), retry at {}: {}",
                msg.orderId(), attempts, cfg.maxAttempts(), next, error.toString());
        return store.save(new FailedOrder(msg, attempts, FailedOrder.Status.RETRYING, describe(error), next, now));
    }

    /** Error de negocio: se guarda para auditoría, sin reintentos. */
    public Mono<Void> reject(OrderMessage msg, Throwable error) {
        log.warn("Order {} rejected: {}", msg.orderId(), error.getMessage());
        return store.save(new FailedOrder(msg, 1, FailedOrder.Status.REJECTED, describe(error), null, clock.instant()));
    }

    /** base * 2^(attempts-1), acotado por maxDelay. */
    Duration backoff(int attempts) {
        long factor = 1L << Math.min(attempts - 1, 20);
        Duration d = cfg.baseDelay().multipliedBy(factor);
        return d.compareTo(cfg.maxDelay()) > 0 ? cfg.maxDelay() : d;
    }

    private static String describe(Throwable e) {
        String m = e.getMessage();
        return e.getClass().getSimpleName() + (m == null ? "" : ": " + m);
    }
}
