package com.example.orders.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("worker")
public record WorkerProperties(
        Kafka kafka,
        Enrichment enrichment,
        Retry retry,
        Lock lock) {

    public record Kafka(
            @DefaultValue("localhost:9092") String bootstrapServers,
            @DefaultValue("orders") String topic,
            @DefaultValue("orders-worker") String groupId,
            @DefaultValue("true") boolean enabled,
            /** Topic para mensajes que no se pudieron procesar ni registrar (dead letter queue). */
            @DefaultValue("orders-dlq") String dlqTopic) {}

    public record Enrichment(
            @DefaultValue("http://localhost:8081") String baseUrl,
            @DefaultValue("2s") Duration timeout,
            /** Reintentos dentro de una misma llamada HTTP (backoff exponencial). */
            @DefaultValue("3") int maxRetries,
            @DefaultValue("200ms") Duration initialBackoff,
            @DefaultValue("2s") Duration maxBackoff,
            @DefaultValue("5m") Duration productCacheTtl,
            @DefaultValue("10000") long productCacheMaxSize) {}

    /** Reintentos "diferidos" de pedidos fallidos, guardados en Redis. */
    public record Retry(
            @DefaultValue("5") int maxAttempts,
            @DefaultValue("5s") Duration baseDelay,
            @DefaultValue("5m") Duration maxDelay,
            @DefaultValue("3s") Duration pollInterval,
            @DefaultValue("50") int batchSize,
            @DefaultValue("7d") Duration failedTtl) {}

    public record Lock(@DefaultValue("60s") Duration ttl) {}
}
