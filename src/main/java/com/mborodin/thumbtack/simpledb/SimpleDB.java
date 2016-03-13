package com.mborodin.thumbtack.simpledb;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class SimpleDB<K, V> implements DB<K, V> {
    public static SimpleDB INSTANCE = new SimpleDB();

    private Map<K, V> workingMemory = new HashMap<K, V>();
    private Deque<Map<K, V>> transactions = new ArrayDeque<Map<K, V>>();

    private SimpleDB() {
    }

    public void begin() {
        throw new UnsupportedOperationException();
    }

    public void rollback() throws NoTransactionException {
        throw new UnsupportedOperationException();
    }

    public void commit() throws NoTransactionException {
        throw new UnsupportedOperationException();
    }

    public void set(K key, V value) {
        throw new UnsupportedOperationException();
    }

    public V get(K key) {
        throw new UnsupportedOperationException();
    }

    public V unset(K key) {
        throw new UnsupportedOperationException();
    }

    public Long countByValue(V value) {
        throw new UnsupportedOperationException();
    }
}
