package com.suitesoftware.psa.catalogservice;

public class AccessException extends Exception {
    public AccessException(String message) {
        super(message);
    }
    public AccessException(String message, Throwable ex) {
        super(message, ex);
    }

}
