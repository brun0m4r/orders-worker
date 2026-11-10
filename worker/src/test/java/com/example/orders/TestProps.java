package com.example.orders;

import com.example.orders.config.WorkerProperties;
import java.time.Duration;

public final class TestProps {
    private TestProps() {}

    public static WorkerProperties with(String baseUrl) {
        return new WorkerProperties(
                new WorkerProperties.Kafka("localhost:9092", "orders", "test", false),
                new WorkerProperties.Enrichment(baseUrl, Duration.ofSeconds(1), 3,
                        Duration.ofMillis(10), Duration.ofMillis(50), Duration.ofMinutes(5), 100),
                new WorkerProperties.Retry(3, Duration.ofSeconds(5), Duration.ofSeconds(30),
                        Duration.ofSeconds(1), 10, Duration.ofDays(1)),
                new WorkerProperties.Lock(Duration.ofSeconds(30)));
    }
}
