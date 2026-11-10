package com.example.orders.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.orders.client.EnrichmentClient;
import com.example.orders.client.EntityNotFoundException;
import com.example.orders.model.ExternalDtos.Customer;
import com.example.orders.model.ExternalDtos.Product;
import com.example.orders.model.OrderMessage;
import com.example.orders.model.OrderValidationException;
import com.example.orders.model.ProcessedOrder;
import com.example.orders.repository.OrderRepository;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class OrderProcessorTest {

    EnrichmentClient client = mock(EnrichmentClient.class);
    OrderRepository repo = mock(OrderRepository.class);
    Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
    OrderProcessor processor = new OrderProcessor(client, repo, clock);

    OrderMessage msg = new OrderMessage("order-123", "customer-456",
            List.of(new OrderMessage.Item("product-789", 2), new OrderMessage.Item("product-790", null)));

    @BeforeEach
    void setUp() {
        when(client.getCustomer("customer-456"))
                .thenReturn(Mono.just(new Customer("customer-456", "Ana", "a@x.com", true)));
        when(client.getProduct("product-789"))
                .thenReturn(Mono.just(new Product("product-789", "Laptop", "d", new BigDecimal("999"), "c")));
        when(client.getProduct("product-790"))
                .thenReturn(Mono.just(new Product("product-790", "Mouse", "d", new BigDecimal("25"), "c")));
        when(repo.save(any(ProcessedOrder.class))).thenAnswer(i -> Mono.just(i.getArgument(0)));
    }

    @Test
    void enrichesAndSavesOrder() {
        StepVerifier.create(processor.process(msg))
                .assertNext(o -> {
                    org.junit.jupiter.api.Assertions.assertEquals("order-123", o.orderId());
                    org.junit.jupiter.api.Assertions.assertEquals("Ana", o.customerName());
                    org.junit.jupiter.api.Assertions.assertEquals(2, o.products().size());
                    org.junit.jupiter.api.Assertions.assertEquals("Laptop", o.products().get(0).name());
                    // 999*2 + 25*1 (cantidad por defecto 1)
                    org.junit.jupiter.api.Assertions.assertEquals(new BigDecimal("2023"), o.total());
                })
                .verifyComplete();
    }

    @Test
    void rejectsInactiveCustomer() {
        when(client.getCustomer("customer-456"))
                .thenReturn(Mono.just(new Customer("customer-456", "Ana", "a@x.com", false)));

        StepVerifier.create(processor.process(msg))
                .expectErrorMatches(e -> e instanceof OrderValidationException && e.getMessage().contains("inactive"))
                .verify();
        verify(repo, never()).save(any());
    }

    @Test
    void rejectsUnknownCustomer() {
        when(client.getCustomer("customer-456")).thenReturn(Mono.error(new EntityNotFoundException("x")));

        StepVerifier.create(processor.process(msg))
                .expectErrorMatches(e -> e instanceof OrderValidationException && e.getMessage().contains("Customer"))
                .verify();
    }

    @Test
    void rejectsUnknownProduct() {
        when(client.getProduct("product-790")).thenReturn(Mono.error(new EntityNotFoundException("x")));

        StepVerifier.create(processor.process(msg))
                .expectErrorMatches(e -> e instanceof OrderValidationException && e.getMessage().contains("product-790"))
                .verify();
        verify(repo, never()).save(any());
    }

    @Test
    void rejectsEmptyProducts() {
        OrderMessage empty = new OrderMessage("o", "c", List.of());
        StepVerifier.create(processor.process(empty))
                .expectError(OrderValidationException.class)
                .verify();
    }

    @Test
    void transientErrorsPropagateUnchanged() {
        when(client.getCustomer("customer-456")).thenReturn(Mono.error(new IllegalStateException("boom")));
        StepVerifier.create(processor.process(msg))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void duplicateOrderIsIdempotent() {
        ProcessedOrder existing = new ProcessedOrder("id", "order-123", "customer-456", "Ana",
                List.of(), BigDecimal.ZERO, clock.instant());
        when(repo.save(any(ProcessedOrder.class))).thenReturn(Mono.error(new DuplicateKeyException("dup")));
        when(repo.findByOrderId("order-123")).thenReturn(Mono.just(existing));

        StepVerifier.create(processor.process(msg)).expectNext(existing).verifyComplete();
    }
}
