package com.example.orders.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.orders.TestProps;
import com.example.orders.dlq.DeadLetterPublisher;
import com.example.orders.model.OrderMessage;
import com.example.orders.service.OrderHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;

class OrderConsumerTest {

    OrderHandler handler = mock(OrderHandler.class);
    DeadLetterPublisher dlq = mock(DeadLetterPublisher.class);
    OrderConsumer consumer = new OrderConsumer(TestProps.with("http://x"), handler, new ObjectMapper(), dlq);

    @SuppressWarnings("unchecked")
    private ReceiverRecord<String, String> record(String key, String value) {
        ReceiverRecord<String, String> r = mock(ReceiverRecord.class);
        when(r.key()).thenReturn(key);
        when(r.value()).thenReturn(value);
        return r;
    }

    @Test
    void validMessageGoesToHandler() {
        when(handler.handle(any(), anyInt())).thenReturn(Mono.empty());
        String json = "{\"orderId\":\"order-1\",\"customerId\":\"c\",\"products\":[{\"productId\":\"p\",\"quantity\":1}]}";

        StepVerifier.create(consumer.process(record("order-1", json))).verifyComplete();

        verify(handler).handle(eq(new OrderMessage("order-1", "c", java.util.List.of(new OrderMessage.Item("p", 1)))), eq(0));
        verify(dlq, never()).publishRaw(any(), any(), any());
    }

    @Test
    void unparseableMessageGoesToDlqAndNotToHandler() {
        when(dlq.publishRaw(any(), any(), any())).thenReturn(Mono.empty());

        StepVerifier.create(consumer.process(record("k", "{no es json"))).verifyComplete();

        verify(dlq).publishRaw(eq("k"), eq("{no es json"), contains("unparseable"));
        verify(handler, never()).handle(any(), anyInt());
    }

    @Test
    void nullPayloadGoesToDlq() {
        when(dlq.publishRaw(any(), any(), any())).thenReturn(Mono.empty());

        StepVerifier.create(consumer.process(record("k", null))).verifyComplete();

        verify(dlq).publishRaw(eq("k"), eq(null), any());
    }

    @Test
    void errorPropagatesWhenDlqFails() {
        when(dlq.publishRaw(any(), any(), any())).thenReturn(Mono.error(new RuntimeException("kafka down")));

        // el error llega al Flux: no se confirma el offset y el consumidor se re-suscribe
        StepVerifier.create(consumer.process(record("k", "{no es json")))
                .expectErrorMessage("kafka down")
                .verify();
    }

    @Test
    void errorPropagatesWhenHandlerFails() {
        when(handler.handle(any(), anyInt())).thenReturn(Mono.error(new RuntimeException("redis and dlq down")));
        String json = "{\"orderId\":\"o\",\"customerId\":\"c\",\"products\":[{\"productId\":\"p\"}]}";

        StepVerifier.create(consumer.process(record("o", json)))
                .expectErrorMessage("redis and dlq down")
                .verify();
    }
}
