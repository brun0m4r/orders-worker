package com.example.orders.failure;

import com.example.orders.config.WorkerProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Persistencia en Redis de pedidos fallidos.
 * - {@code orders:failed:<orderId>}: JSON del FailedOrder (con TTL).
 * - {@code orders:failed:retry-queue}: ZSET orderId -> epochMillis del próximo reintento.
 */
@Component
public class FailedOrderStore {

    static final String KEY_PREFIX = "orders:failed:";
    static final String QUEUE = "orders:failed:retry-queue";

    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final WorkerProperties.Retry cfg;

    public FailedOrderStore(ReactiveStringRedisTemplate redis, ObjectMapper mapper, WorkerProperties props) {
        this.redis = redis;
        this.mapper = mapper;
        this.cfg = props.retry();
    }

    public Mono<Void> save(FailedOrder failed) {
        String id = failed.orderId();
        Mono<?> value = Mono.fromCallable(() -> mapper.writeValueAsString(failed))
                .flatMap(json -> redis.opsForValue().set(KEY_PREFIX + id, json, cfg.failedTtl()));
        Mono<?> index = failed.status() == FailedOrder.Status.RETRYING
                ? redis.opsForZSet().add(QUEUE, id, failed.nextRetryAt().toEpochMilli())
                : redis.opsForZSet().remove(QUEUE, id);
        return value.then(index).then();
    }

    /** Pedidos cuyo próximo reintento ya venció. */
    public Flux<FailedOrder> findDue(long nowMillis, int limit) {
        return redis.opsForZSet()
                .rangeByScore(QUEUE, Range.closed(0d, (double) nowMillis), Limit.limit().count(limit))
                .flatMap(id -> redis.opsForValue().get(KEY_PREFIX + id)
                        // la clave expiró pero quedó en el índice: limpiar
                        .switchIfEmpty(redis.opsForZSet().remove(QUEUE, id).then(Mono.empty())))
                .flatMap(json -> Mono.fromCallable(() -> mapper.readValue(json, FailedOrder.class)));
    }

    public Mono<FailedOrder> find(String orderId) {
        return redis.opsForValue().get(KEY_PREFIX + orderId).flatMap(this::parse);
    }

    /** Se llama tras procesar con éxito. */
    public Mono<Void> remove(String orderId) {
        return redis.delete(KEY_PREFIX + orderId)
                .then(redis.opsForZSet().remove(QUEUE, orderId))
                .then();
    }

    private Mono<FailedOrder> parse(String json) {
        try {
            return Mono.just(mapper.readValue(json, FailedOrder.class));
        } catch (JsonProcessingException e) {
            return Mono.error(e);
        }
    }
}
