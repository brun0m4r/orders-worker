package com.example.orders.client;

import com.example.orders.config.WorkerProperties;
import com.example.orders.model.ExternalDtos.Customer;
import com.example.orders.model.ExternalDtos.Product;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Cliente de la API Go. Reintenta con backoff exponencial solo errores transitorios
 * (timeouts, conexión, 5xx, 429). Un 404 se traduce a {@link EntityNotFoundException}.
 * Los productos se cachean (catálogo cambia poco); los clientes NO, porque "activo" puede cambiar.
 */
@Component
public class EnrichmentClient {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentClient.class);

    private final WebClient webClient;
    private final WorkerProperties.Enrichment cfg;
    private final Cache<String, Mono<Product>> productCache;

    public EnrichmentClient(WebClient.Builder builder, WorkerProperties props) {
        this.cfg = props.enrichment();
        this.webClient = builder.baseUrl(cfg.baseUrl()).build();
        this.productCache = Caffeine.newBuilder()
                .maximumSize(cfg.productCacheMaxSize())
                .expireAfterWrite(cfg.productCacheTtl())
                .build();
    }

    public Mono<Product> getProduct(String productId) {
        return Mono.defer(() -> productCache.get(productId, id ->
                fetch("/products/{id}", id, Product.class, "product")
                        // cachea solo éxitos; errores no se memorizan
                        .cache(v -> cfg.productCacheTtl(), e -> Duration.ZERO, () -> Duration.ZERO)));
    }

    public Mono<Customer> getCustomer(String customerId) {
        return fetch("/customers/{id}", customerId, Customer.class, "customer");
    }

    private <T> Mono<T> fetch(String uri, String id, Class<T> type, String kind) {
        return webClient.get()
                .uri(uri, id)
                .retrieve()
                .onStatus(s -> s.value() == 404,
                        r -> Mono.error(new EntityNotFoundException(kind + " " + id + " not found")))
                .bodyToMono(type)
                .timeout(cfg.timeout())
                .retryWhen(Retry.backoff(cfg.maxRetries(), cfg.initialBackoff())
                        .maxBackoff(cfg.maxBackoff())
                        .jitter(0.3)
                        .filter(EnrichmentClient::isTransient)
                        .doBeforeRetry(s -> log.warn("Retry {} for {} {}: {}",
                                s.totalRetries() + 1, kind, id, s.failure().toString()))
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()));
    }

    static boolean isTransient(Throwable t) {
        if (t instanceof WebClientResponseException e) {
            return e.getStatusCode().is5xxServerError() || e.getStatusCode().value() == 429;
        }
        return t instanceof WebClientRequestException || t instanceof TimeoutException;
    }
}
