package com.mborodin.thumbtack.simpledb;

public class NoTransactionException extends Exception {
    public NoTransactionException(String message) {
        super(message);
    }
}
