package com.example.orders.lock;

import com.example.orders.config.WorkerProperties;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Lock distribuido sobre Redis: SET key token NX PX ttl.
 * Se libera con un script Lua que verifica el token, para no borrar el lock de otra instancia
 * si el nuestro ya expiró.
 */
@Component
public class DistributedLock {

    private static final Logger log = LoggerFactory.getLogger(DistributedLock.class);

    private static final RedisScript<Long> RELEASE = RedisScript.of(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long.class);

    private final ReactiveStringRedisTemplate redis;
    private final WorkerProperties.Lock cfg;

    public DistributedLock(ReactiveStringRedisTemplate redis, WorkerProperties props) {
        this.redis = redis;
        this.cfg = props.lock();
    }

    /**
     * Ejecuta {@code action} con el lock de {@code name}. Si otra instancia lo tiene,
     * emite {@link LockNotAcquiredException}.
     */
    public <T> Mono<T> withLock(String name, Supplier<Mono<T>> action) {
        String key = "lock:order:" + name;
        String token = UUID.randomUUID().toString();
        return Mono.usingWhen(
                redis.opsForValue().setIfAbsent(key, token, cfg.ttl())
                        .flatMap(ok -> Boolean.TRUE.equals(ok)
                                ? Mono.just(token)
                                : Mono.error(new LockNotAcquiredException(key))),
                t -> action.get(),
                t -> release(key, t));
    }

    private Mono<Void> release(String key, String token) {
        return redis.execute(RELEASE, List.of(key), List.of(token))
                .then()
                .onErrorResume(e -> {
                    log.warn("Could not release lock {}: {}", key, e.toString());
                    return Mono.empty();
                });
    }
}
