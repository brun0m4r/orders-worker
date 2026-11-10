package com.example.orders.repository;

import com.example.orders.model.ProcessedOrder;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

public interface OrderRepository extends ReactiveMongoRepository<ProcessedOrder, String> {
    Mono<ProcessedOrder> findByOrderId(String orderId);
}
