package com.mborodin.thumbtack.simpledb;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class SimpleDB<K, V> implements DB<K, V> {
    public static final SimpleDB INSTANCE = new SimpleDB();

    private final Map<K, V> workingMemory = new HashMap<K, V>();
    private final Deque<Map<K, V>> transactions = new ArrayDeque<Map<K, V>>();
    private final Map<V, Long> countByValue = new HashMap<V, Long>();

    private SimpleDB() {
    }

    public void restart() {
        workingMemory.clear();
        transactions.clear();
        countByValue.clear();
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
        V previousValue = workingMemory.put(key, value);
        updateCountByValue(previousValue, -1);
        updateCountByValue(value, 1);
        saveInTransaction(key, previousValue);
    }

    public V get(K key) {
        return workingMemory.get(key);
    }

    public V unset(K key) {
        V previousValue = workingMemory.remove(key);
        saveInTransaction(key, previousValue);
        updateCountByValue(previousValue, -1);
        return previousValue;
    }

    public Long countByValue(V value) {
        if (countByValue.containsKey(value)) {
            return countByValue.get(value);
        }
        return 0L;
    }

    private void saveInTransaction(K key, V value) {
        if (!transactions.isEmpty() && !transactions.getLast().containsKey(key)) {
            transactions.getLast().put(key, value);
        }
    }

    private void updateCountByValue(V value, int change) {
        Long count = countByValue.get(value);
        if (count == null) {
            if (change < 0) {
                return;
            }
            count = new Long(0);
        }
        count += change;
        countByValue.put(value, count);
    }
}
