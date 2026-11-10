package com.example.orders.client;

/** La API de enriquecimiento respondió 404. */
public class EntityNotFoundException extends RuntimeException {
    public EntityNotFoundException(String message) {
        super(message);
    }
}
