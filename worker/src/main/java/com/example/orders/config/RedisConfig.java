package com.example.orders.config;

import io.lettuce.core.ClientOptions;
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisConfig {

    /**
     * Por defecto Lettuce encola los comandos mientras Redis está desconectado y los ejecuta al reconectar,
     * así que una caída deja el consumidor colgado hasta el timeout. Con REJECT_COMMANDS fallan al instante
     * y el pedido sigue el camino de error (reintento en Redis o DLQ) en lugar de bloquear la partición.
     * Se reconecta solo en segundo plano.
     */
    @Bean
    LettuceClientConfigurationBuilderCustomizer failFastWhenDisconnected() {
        return builder -> builder.clientOptions(ClientOptions.builder()
                .autoReconnect(true)
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .build());
    }
}
