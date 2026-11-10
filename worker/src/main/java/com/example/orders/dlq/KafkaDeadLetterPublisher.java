package com.example.orders.dlq;

import com.example.orders.config.WorkerProperties;
import com.example.orders.model.OrderMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Component
public class KafkaDeadLetterPublisher implements DeadLetterPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaDeadLetterPublisher.class);
    private static final int MAX_REASON_LENGTH = 500;

    private final KafkaSender<String, String> sender;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final String topic;

    public KafkaDeadLetterPublisher(KafkaSender<String, String> sender, ObjectMapper mapper,
                                    Clock clock, WorkerProperties props) {
        this.sender = sender;
        this.mapper = mapper;
        this.clock = clock;
        this.topic = props.kafka().dlqTopic();
    }

    @Override
    public Mono<Void> publish(OrderMessage message, String reason) {
        return Mono.fromCallable(() -> mapper.writeValueAsString(message))
                .flatMap(json -> publishRaw(message.orderId(), json, reason));
    }

    @Override
    public Mono<Void> publishRaw(String key, String payload, String reason) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload);
        record.headers().add("dlq-reason", truncate(reason).getBytes(StandardCharsets.UTF_8));
        record.headers().add("dlq-at", clock.instant().toString().getBytes(StandardCharsets.UTF_8));

        return sender.send(Mono.just(SenderRecord.create(record, key)))
                // con stopOnError el Flux ya falla solo; esto cubre el caso en que reporte la excepción en el resultado
                .concatMap(r -> r.exception() != null ? Mono.<Void>error(r.exception()) : Mono.<Void>empty())
                .then()
                .doOnSuccess(v -> log.warn("Message {} sent to DLQ '{}': {}", key, topic, reason));
    }

    /** Una sola línea (los mensajes de error pueden traer saltos de línea) y acotada en tamaño. */
    private static String truncate(String s) {
        if (s == null) return "";
        String oneLine = s.replaceAll("\\s+", " ").trim();
        return oneLine.length() <= MAX_REASON_LENGTH ? oneLine : oneLine.substring(0, MAX_REASON_LENGTH);
    }
}
