package com.example.orders.kafka;

import com.example.orders.config.WorkerProperties;
import com.example.orders.model.OrderMessage;
import com.example.orders.service.OrderHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
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
    private Disposable subscription;

    public OrderConsumer(WorkerProperties props, OrderHandler handler, ObjectMapper mapper) {
        this.cfg = props.kafka();
        this.handler = handler;
        this.mapper = mapper;
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
                .doOnError(e -> log.error("Kafka consumer error", e))
                .retry() // si el stream cae, se vuelve a suscribir
                .subscribe();
        log.info("Consuming topic '{}' as group '{}'", cfg.topic(), cfg.groupId());
    }

    private Mono<Void> handleRecord(ReceiverRecord<String, String> record) {
        return parse(record)
                .flatMap(msg -> handler.handle(msg, 0))
                .doFinally(s -> record.receiverOffset().acknowledge());
    }

    private Mono<OrderMessage> parse(ReceiverRecord<String, String> record) {
        try {
            OrderMessage msg = mapper.readValue(record.value(), OrderMessage.class);
            return msg == null ? Mono.empty() : Mono.just(msg);
        } catch (Exception e) {
            // mensaje ilegible: no tiene sentido reintentarlo
            log.error("Discarding unparseable message at {}-{}@{}: {}",
                    record.topic(), record.partition(), record.offset(), e.getMessage());
            return Mono.empty();
        }
    }

    @PreDestroy
    void stop() {
        if (subscription != null) subscription.dispose();
    }
}
