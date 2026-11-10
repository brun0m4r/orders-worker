package com.example.orders.model;

/** Error de negocio permanente (cliente/producto inexistente, cliente inactivo, mensaje inválido). No se reintenta. */
public class OrderValidationException extends RuntimeException {
    public OrderValidationException(String message) {
        super(message);
    }
}
