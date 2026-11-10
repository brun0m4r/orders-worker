package com.example.orders.kafka;

import com.example.orders.config.WorkerProperties;
import com.example.orders.dlq.DeadLetterPublisher;
import com.example.orders.model.OrderMessage;
import com.example.orders.service.OrderHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

/**
 * Consume el tópico de pedidos. Procesa en paralelo entre particiones y en orden dentro de cada una.
 * El offset se confirma después de manejar el pedido (éxito, o fallo ya registrado en Redis),
 * así un crash antes de procesar provoca redelivery (at-least-once; el proceso es idempotente).
 */
@Component
@ConditionalOnProperty(name = "worker.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);
    /** Máximo de particiones asignadas simultáneamente a esta instancia. */
    private static final int MAX_PARTITIONS = 256;

    private final WorkerProperties.Kafka cfg;
    private final OrderHandler handler;
    private final ObjectMapper mapper;
    private final DeadLetterPublisher deadLetters;
    private Disposable subscription;

    public OrderConsumer(WorkerProperties props, OrderHandler handler, ObjectMapper mapper,
                         DeadLetterPublisher deadLetters) {
        this.cfg = props.kafka();
        this.handler = handler;
        this.mapper = mapper;
        this.deadLetters = deadLetters;
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        ReceiverOptions<String, String> options = ReceiverOptions.<String, String>create(Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, cfg.groupId(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))
                .subscription(List.of(cfg.topic()));

        subscription = KafkaReceiver.create(options).receive()
                .groupBy(r -> r.receiverOffset().topicPartition())
                .flatMap(partition -> partition.concatMap(this::handleRecord), MAX_PARTITIONS)
                .doOnError(e -> log.error("Kafka consumer error, will resubscribe (offset not committed)", e))
                // si el stream cae (p. ej. Redis y DLQ caídos), se vuelve a suscribir con espera creciente;
                // Kafka reentrega desde el último offset confirmado, así que no se pierde ningún pedido
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(30)))
                .subscribe();
        log.info("Consuming topic '{}' as group '{}'", cfg.topic(), cfg.groupId());
    }

    /** El offset se confirma solo si el mensaje quedó manejado (procesado, en Redis o en el DLQ). */
    private Mono<Void> handleRecord(ReceiverRecord<String, String> record) {
        return process(record)
                .then(Mono.<Void>fromRunnable(() -> record.receiverOffset().acknowledge()));
    }

    Mono<Void> process(ReceiverRecord<String, String> record) {
        OrderMessage msg;
        try {
            msg = mapper.readValue(record.value(), OrderMessage.class);
        } catch (Exception e) {
            return toDeadLetter(record, "unparseable: " + e.getMessage());
        }
        if (msg == null) {
            return toDeadLetter(record, "empty payload");
        }
        return handler.handle(msg, 0);
    }

    // mensaje ilegible: no tiene sentido reintentarlo, pero tampoco perderlo
    private Mono<Void> toDeadLetter(ReceiverRecord<String, String> record, String reason) {
        log.error("Unprocessable message at {}-{}@{}, sending to DLQ: {}",
                record.topic(), record.partition(), record.offset(), reason);
        return deadLetters.publishRaw(record.key(), record.value(), reason);
    }

    @PreDestroy
    void stop() {
        if (subscription != null) subscription.dispose();
    }
}
