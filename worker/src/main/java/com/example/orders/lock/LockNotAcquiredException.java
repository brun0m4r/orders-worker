package com.example.orders.lock;

public class LockNotAcquiredException extends RuntimeException {
    public LockNotAcquiredException(String key) {
        super("Lock already held: " + key);
    }
}
