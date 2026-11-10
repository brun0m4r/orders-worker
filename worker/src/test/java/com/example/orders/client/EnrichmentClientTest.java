package com.example.orders.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.example.orders.TestProps;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.test.StepVerifier;

class EnrichmentClientTest {

    MockWebServer server;
    EnrichmentClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        client = new EnrichmentClient(WebClient.builder(), TestProps.with(server.url("/").toString()));
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    private static MockResponse json(String body) {
        return new MockResponse().setHeader("Content-Type", "application/json").setBody(body);
    }

    @Test
    void retriesTransientErrorsWithBackoff() {
        server.enqueue(new MockResponse().setResponseCode(503));
        server.enqueue(new MockResponse().setResponseCode(500));
        server.enqueue(json("{\"id\":\"c1\",\"name\":\"Ana\",\"active\":true}"));

        StepVerifier.create(client.getCustomer("c1"))
                .assertNext(c -> assertEquals("Ana", c.name()))
                .verifyComplete();
        assertEquals(3, server.getRequestCount());
    }

    @Test
    void givesUpAfterMaxRetriesAndPropagatesOriginalError() {
        for (int i = 0; i < 4; i++) server.enqueue(new MockResponse().setResponseCode(503));

        StepVerifier.create(client.getCustomer("c1"))
                .expectError(WebClientResponseException.ServiceUnavailable.class)
                .verify();
        assertEquals(4, server.getRequestCount()); // 1 intento + 3 reintentos
    }

    @Test
    void notFoundIsNotRetried() {
        server.enqueue(new MockResponse().setResponseCode(404));

        StepVerifier.create(client.getProduct("nope"))
                .expectError(EntityNotFoundException.class)
                .verify();
        assertEquals(1, server.getRequestCount());
    }

    @Test
    void productsAreCached() {
        server.enqueue(json("{\"id\":\"p1\",\"name\":\"Laptop\",\"price\":999}"));

        StepVerifier.create(client.getProduct("p1")).expectNextCount(1).verifyComplete();
        StepVerifier.create(client.getProduct("p1")).expectNextCount(1).verifyComplete();
        assertEquals(1, server.getRequestCount());
    }

    @Test
    void failuresAreNotCached() {
        server.enqueue(new MockResponse().setResponseCode(404));
        server.enqueue(json("{\"id\":\"p1\",\"name\":\"Laptop\",\"price\":999}"));

        StepVerifier.create(client.getProduct("p1")).expectError(EntityNotFoundException.class).verify();
        StepVerifier.create(client.getProduct("p1")).expectNextCount(1).verifyComplete();
    }
}
