package com.example.orders.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

/** Productor usado solo para la cola de mensajes muertos (DLQ). El productor interno se crea al primer envío. */
@Configuration
public class KafkaProducerConfig {

    @Bean(destroyMethod = "close")
    KafkaSender<String, String> dlqSender(WorkerProperties props) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.kafka().bootstrapServers());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // si Kafka no responde, que el error aparezca rápido (el consumidor no confirmará el offset)
        cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15_000);
        cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        return KafkaSender.create(SenderOptions.create(cfg));
    }
}
