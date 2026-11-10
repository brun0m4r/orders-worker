package com.example.orders.failure;

import com.example.orders.config.WorkerProperties;
import com.example.orders.service.OrderHandler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Sondea Redis cada {@code pollInterval} y reprocesa los pedidos cuyo reintento venció.
 * Es seguro con varias instancias: el lock por pedido evita el doble procesamiento.
 */
@Component
public class RetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetryScheduler.class);

    private final FailedOrderStore store;
    private final OrderHandler handler;
    private final WorkerProperties.Retry cfg;
    private final Clock clock;
    private Disposable subscription;

    public RetryScheduler(FailedOrderStore store, OrderHandler handler, WorkerProperties props, Clock clock) {
        this.store = store;
        this.handler = handler;
        this.cfg = props.retry();
        this.clock = clock;
    }

    @PostConstruct
    void start() {
        subscription = Flux.interval(cfg.pollInterval())
                .onBackpressureDrop()
                .concatMap(tick -> runOnce()
                        .onErrorResume(e -> {
                            log.error("Retry cycle failed: {}", e.toString());
                            return Mono.empty();
                        }))
                .subscribe();
    }

    @PreDestroy
    void stop() {
        if (subscription != null) subscription.dispose();
    }

    Mono<Void> runOnce() {
        return store.findDue(clock.millis(), cfg.batchSize())
                .flatMap(f -> handler.handle(f.message(), f.attempts()), 4)
                .then();
    }
}
