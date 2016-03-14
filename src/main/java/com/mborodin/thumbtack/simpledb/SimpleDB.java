package com.mborodin.thumbtack.simpledb;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of in-memory DB for Thumbtack's Simple Database Challenge. It's not a thread-safe
 * implementation.
 * <br/>
 * Amortized time complexity of operations:
 * <ul>
 *     <li>Begin — O(1)</li>
 *     <li>GET — O(1). O(log n) in the worst case. Note: It's not a O(n) in the worst case,
 *     cause HashMap switches from an array to a TreeMap as an internal bucket store,
 *     when there're too many items. </li>
 *     <li>SET — O(1). O(log n) in the worst case.</li>
 *     <li>UNSET — O(1). O(log n) in the worst case.</li>
 *     <li>NUMEQUALTO — O(1). O(log n) in the worst case.</li>
 *     <li>COMMIT — O(1)</li>
 *     <li>ROLLBACK — O(n), where n is a number of key stored in a previous transaction.</li>
 * <ul/>
 *
 * @param <K> type of keys. Should have good hash functions and be immutable.
 * @param <V> type of values. Should have good hash functions and be immutable.
 */
public class SimpleDB<K, V> implements DB<K, V> {
    // DB should be a singleton.
    public static final SimpleDB INSTANCE = new SimpleDB();

    // This's a working copy of data.
    private Map<K, V> workingMemory = new HashMap<>();

    /**
     * To support nested transactions we will store key-value mapping from a previous committed
     * state in this stack.
     */
    private Deque<Map<K, V>> transactions = new ArrayDeque<>();

    // Index for getting count by value
    private Map<V, Long> countByValue = new HashMap<>();

    private SimpleDB() {
    }

    /**
     * Currently we need that for the tests only.
     */
    public void restart() {
        workingMemory = new HashMap<>();
        transactions = new ArrayDeque<>();
        countByValue = new HashMap<>();
    }

    public void begin() {
        transactions.push(new HashMap<>());
    }

    public void rollback() throws NoTransactionException {
        if (transactions.isEmpty()) {
            throw new NoTransactionException("ROLLBACK: No opened transactions");
        }
        transactions.pop().forEach((k, v) -> {
                    if (v == null) {
                        V pValue = workingMemory.remove(k);
                        if (pValue != null) {
                            updateCountByValue(pValue, -1);
                        }
                    } else {
                        V pValue = workingMemory.put(k, v);
                        updateCountByValue(v, 1);
                        if (pValue != null) {
                            updateCountByValue(pValue, -1);
                        }
                    }
                }
        );
    }

    public void commit() throws NoTransactionException {
        if (transactions.isEmpty()) {
            throw new NoTransactionException("COMMIT: No opened transactions");
        }
        transactions = new ArrayDeque<>();
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

    /**
     * Stores previous key-value mapping to support nested transactions.
     * Does nothing, if it's not a transaction.
     *
     * @param key
     * @param value
     */
    private void saveInTransaction(K key, V value) {
        if (!transactions.isEmpty() && !transactions.peek().containsKey(key)) {
            transactions.peek().put(key, value);
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
