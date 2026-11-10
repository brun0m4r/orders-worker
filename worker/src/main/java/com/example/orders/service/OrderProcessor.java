package com.example.orders.service;

import com.example.orders.client.EnrichmentClient;
import com.example.orders.client.EntityNotFoundException;
import com.example.orders.model.ExternalDtos.Customer;
import com.example.orders.model.OrderMessage;
import com.example.orders.model.OrderValidationException;
import com.example.orders.model.ProcessedOrder;
import com.example.orders.repository.OrderRepository;
import java.math.BigDecimal;
import java.time.Clock;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Valida, enriquece y persiste un pedido. Idempotente por orderId (índice único en Mongo). */
@Service
public class OrderProcessor {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessor.class);
    private static final int PRODUCT_CONCURRENCY = 8;

    private final EnrichmentClient client;
    private final OrderRepository repository;
    private final Clock clock;

    public OrderProcessor(EnrichmentClient client, OrderRepository repository, Clock clock) {
        this.client = client;
        this.repository = repository;
        this.clock = clock;
    }

    public Mono<ProcessedOrder> process(OrderMessage msg) {
        return Mono.defer(() -> {
            validateMessage(msg);
            // cliente y productos se consultan en paralelo; ambos deben ser válidos
            Mono<Customer> customer = client.getCustomer(msg.customerId())
                    .onErrorMap(EntityNotFoundException.class,
                            e -> new OrderValidationException("Customer not found: " + msg.customerId()))
                    .flatMap(c -> c.active()
                            ? Mono.just(c)
                            : Mono.error(new OrderValidationException("Customer inactive: " + c.id())));

            Mono<List<ProcessedOrder.Line>> lines = Flux.fromIterable(msg.products())
                    .flatMapSequential(item -> client.getProduct(item.productId())
                            .onErrorMap(EntityNotFoundException.class,
                                    e -> new OrderValidationException("Product not found: " + item.productId()))
                            .map(p -> new ProcessedOrder.Line(
                                    item.productId(), p.name(), p.description(), p.price(), item.quantityOrDefault())),
                            PRODUCT_CONCURRENCY)
                    .collectList();

            return Mono.zip(customer, lines)
                    .map(t -> toDocument(msg, t.getT1(), t.getT2()))
                    .flatMap(this::saveIdempotent);
        });
    }

    private void validateMessage(OrderMessage msg) {
        if (isBlank(msg.orderId())) throw new OrderValidationException("orderId is required");
        if (isBlank(msg.customerId())) throw new OrderValidationException("customerId is required");
        if (msg.products() == null || msg.products().isEmpty())
            throw new OrderValidationException("products must not be empty");
        for (OrderMessage.Item i : msg.products()) {
            if (i == null || isBlank(i.productId())) throw new OrderValidationException("productId is required");
            if (i.quantityOrDefault() <= 0) throw new OrderValidationException("quantity must be > 0");
        }
    }

    private ProcessedOrder toDocument(OrderMessage msg, Customer customer, List<ProcessedOrder.Line> lines) {
        BigDecimal total = lines.stream()
                .map(l -> l.price().multiply(BigDecimal.valueOf(l.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        return new ProcessedOrder(null, msg.orderId(), msg.customerId(), customer.name(), lines, total, clock.instant());
    }

    private Mono<ProcessedOrder> saveIdempotent(ProcessedOrder order) {
        return repository.save(order)
                .onErrorResume(DuplicateKeyException.class, e -> {
                    log.info("Order {} already processed, skipping", order.orderId());
                    return repository.findByOrderId(order.orderId());
                });
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
