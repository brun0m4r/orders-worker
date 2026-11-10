package com.example.orders.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.orders.dlq.DeadLetterPublisher;
import com.example.orders.failure.FailedOrderStore;
import com.example.orders.failure.FailureService;
import com.example.orders.lock.DistributedLock;
import com.example.orders.lock.LockNotAcquiredException;
import com.example.orders.model.OrderMessage;
import com.example.orders.model.OrderValidationException;
import com.example.orders.model.ProcessedOrder;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class OrderHandlerTest {

    OrderProcessor processor = mock(OrderProcessor.class);
    DistributedLock lock = mock(DistributedLock.class);
    FailureService failures = mock(FailureService.class);
    FailedOrderStore store = mock(FailedOrderStore.class);
    DeadLetterPublisher deadLetters = mock(DeadLetterPublisher.class);
    OrderHandler handler = new OrderHandler(processor, lock, failures, store, deadLetters);

    OrderMessage msg = new OrderMessage("order-1", "c", List.of(new OrderMessage.Item("p", 1)));

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        // lock "transparente": ejecuta la acción directamente
        when(lock.withLock(eq("order-1"), any())).thenAnswer(i -> ((Supplier<Mono<?>>) i.getArgument(1)).get());
        when(store.remove(any())).thenReturn(Mono.empty());
        when(failures.recordFailure(any(), any(), anyInt())).thenReturn(Mono.empty());
        when(failures.reject(any(), any())).thenReturn(Mono.empty());
    }

    @Test
    void successClearsFailedState() {
        when(processor.process(msg)).thenReturn(Mono.just(mock(ProcessedOrder.class)));

        StepVerifier.create(handler.handle(msg, 0)).verifyComplete();
        verify(store).remove("order-1");
        verify(failures, never()).recordFailure(any(), any(), anyInt());
    }

    @Test
    void validationErrorIsRejectedNotRetried() {
        when(processor.process(msg)).thenReturn(Mono.error(new OrderValidationException("bad")));

        StepVerifier.create(handler.handle(msg, 0)).verifyComplete();
        verify(failures).reject(eq(msg), any(OrderValidationException.class));
        verify(failures, never()).recordFailure(any(), any(), anyInt());
    }

    @Test
    void transientErrorIsRecordedWithPreviousAttempts() {
        when(processor.process(msg)).thenReturn(Mono.error(new IllegalStateException("down")));

        StepVerifier.create(handler.handle(msg, 2)).verifyComplete();
        verify(failures).recordFailure(eq(msg), any(IllegalStateException.class), eq(2));
    }

    @Test
    void lockHeldByAnotherInstanceIsSkipped() {
        when(lock.withLock(eq("order-1"), any())).thenReturn(Mono.error(new LockNotAcquiredException("k")));

        StepVerifier.create(handler.handle(msg, 0)).verifyComplete();
        verify(processor, never()).process(any());
        verify(failures, never()).recordFailure(any(), any(), anyInt());
        verify(failures, never()).reject(any(), any());
    }

    @Test
    void redisFailureWhileRecordingSendsOrderToDlq() {
        when(processor.process(msg)).thenReturn(Mono.error(new IllegalStateException("down")));
        when(failures.recordFailure(any(), any(), anyInt())).thenReturn(Mono.error(new RuntimeException("redis down")));
        when(deadLetters.publish(eq(msg), any())).thenReturn(Mono.empty());

        StepVerifier.create(handler.handle(msg, 0)).verifyComplete();
        verify(deadLetters).publish(eq(msg), org.mockito.ArgumentMatchers.contains("failure-store-unavailable"));
    }

    @Test
    void errorPropagatesWhenDlqAlsoFails() {
        when(processor.process(msg)).thenReturn(Mono.error(new IllegalStateException("down")));
        when(failures.recordFailure(any(), any(), anyInt())).thenReturn(Mono.error(new RuntimeException("redis down")));
        when(deadLetters.publish(eq(msg), any())).thenReturn(Mono.error(new RuntimeException("kafka down")));

        // el consumidor no debe confirmar el offset: el error tiene que llegar hasta él
        StepVerifier.create(handler.handle(msg, 0))
                .expectErrorMessage("kafka down")
                .verify();
    }

    @Test
    void dlqIsNotUsedWhenFailureIsRecordedInRedis() {
        when(processor.process(msg)).thenReturn(Mono.error(new IllegalStateException("down")));

        StepVerifier.create(handler.handle(msg, 0)).verifyComplete();
        verify(deadLetters, never()).publish(any(), any());
    }
}
