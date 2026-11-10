package com.example.orders.failure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.orders.TestProps;
import com.example.orders.model.OrderMessage;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

class FailureServiceTest {

    static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    FailedOrderStore store = mock(FailedOrderStore.class);
    // maxAttempts=3, baseDelay=5s, maxDelay=30s (ver TestProps)
    FailureService service = new FailureService(store, TestProps.with("http://x"), Clock.fixed(NOW, ZoneOffset.UTC));
    OrderMessage msg = new OrderMessage("order-1", "c", List.of(new OrderMessage.Item("p", 1)));

    private FailedOrder saved() {
        ArgumentCaptor<FailedOrder> cap = ArgumentCaptor.forClass(FailedOrder.class);
        verify(store).save(cap.capture());
        return cap.getValue();
    }

    @Test
    void firstFailureSchedulesRetryWithBaseDelay() {
        when(store.save(any())).thenReturn(Mono.empty());
        service.recordFailure(msg, new RuntimeException("x"), 0).block();

        FailedOrder f = saved();
        assertEquals(FailedOrder.Status.RETRYING, f.status());
        assertEquals(1, f.attempts());
        assertEquals(NOW.plusSeconds(5), f.nextRetryAt());
    }

    @Test
    void secondFailureDoublesDelay() {
        when(store.save(any())).thenReturn(Mono.empty());
        service.recordFailure(msg, new RuntimeException("x"), 1).block();

        FailedOrder f = saved();
        assertEquals(2, f.attempts());
        assertEquals(NOW.plusSeconds(10), f.nextRetryAt());
    }

    @Test
    void marksDeadWhenAttemptsExhausted() {
        when(store.save(any())).thenReturn(Mono.empty());
        service.recordFailure(msg, new RuntimeException("x"), 2).block();

        FailedOrder f = saved();
        assertEquals(FailedOrder.Status.DEAD, f.status());
        assertEquals(3, f.attempts());
    }

    @Test
    void backoffIsCappedAtMaxDelay() {
        assertEquals(Duration.ofSeconds(30), service.backoff(10));
    }

    @Test
    void rejectStoresWithoutRetry() {
        when(store.save(any())).thenReturn(Mono.empty());
        service.reject(msg, new RuntimeException("bad")).block();

        assertEquals(FailedOrder.Status.REJECTED, saved().status());
    }
}
