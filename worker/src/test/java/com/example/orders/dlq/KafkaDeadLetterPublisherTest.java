package com.example.orders.dlq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.example.orders.TestProps;
import com.example.orders.model.OrderMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

class KafkaDeadLetterPublisherTest {

    @SuppressWarnings("unchecked")
    KafkaSender<String, String> sender = mock(KafkaSender.class);
    List<ProducerRecord<String, String>> sent = new ArrayList<>();
    KafkaDeadLetterPublisher publisher = new KafkaDeadLetterPublisher(
            sender, new ObjectMapper(),
            Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC),
            TestProps.with("http://x"));

    /** Simula un envío exitoso, capturando el registro publicado. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void senderSucceeds() {
        when(sender.send(any(Publisher.class))).thenAnswer(inv ->
                Flux.from((Publisher<SenderRecord<String, String, String>>) inv.getArgument(0))
                        .doOnNext(r -> sent.add(r))
                        .map(r -> mock(SenderResult.class)));
    }

    private static String header(ProducerRecord<String, String> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    @Test
    void publishesRawPayloadToDlqTopicWithReasonHeader() {
        senderSucceeds();

        StepVerifier.create(publisher.publishRaw("order-9", "{rotto", "unparseable: boom")).verifyComplete();

        assertEquals(1, sent.size());
        ProducerRecord<String, String> r = sent.get(0);
        assertEquals("orders-dlq", r.topic());
        assertEquals("order-9", r.key());
        assertEquals("{rotto", r.value());
        assertEquals("unparseable: boom", header(r, "dlq-reason"));
        assertEquals("2026-01-01T00:00:00Z", header(r, "dlq-at"));
    }

    @Test
    void publishesParsedOrderAsJsonKeyedByOrderId() {
        senderSucceeds();
        OrderMessage msg = new OrderMessage("order-1", "customer-456", List.of(new OrderMessage.Item("p1", 2)));

        StepVerifier.create(publisher.publish(msg, "redis down")).verifyComplete();

        ProducerRecord<String, String> r = sent.get(0);
        assertEquals("order-1", r.key());
        assertTrue(r.value().contains("\"orderId\":\"order-1\""));
        assertTrue(r.value().contains("\"productId\":\"p1\""));
    }

    @Test
    void truncatesVeryLongReason() {
        senderSucceeds();

        StepVerifier.create(publisher.publishRaw("k", "v", "x".repeat(2000))).verifyComplete();

        assertEquals(500, header(sent.get(0), "dlq-reason").length());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void propagatesErrorWhenSendFails() {
        when(sender.send(any(Publisher.class))).thenReturn(Flux.error(new RuntimeException("broker down")));

        StepVerifier.create(publisher.publishRaw("k", "v", "r"))
                .expectErrorMessage("broker down")
                .verify();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void propagatesErrorReportedInSendResult() {
        SenderResult<String> failed = mock(SenderResult.class);
        when(failed.exception()).thenReturn(new RuntimeException("not enough replicas"));
        when(sender.send(any(Publisher.class))).thenReturn(Flux.just(failed));

        StepVerifier.create(publisher.publishRaw("k", "v", "r"))
                .expectErrorMessage("not enough replicas")
                .verify();
    }
}
