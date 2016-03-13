package com.mborodin.thumbtack.simpledb;

public interface DB<K, V> {
    /**
     * Opens a new transaction block. Transaction blocks can be nested;
     * a BEGIN can be issued inside of an existing block.
     *
     * Should have an average-case runtime of O(log N) or better.
     */
    void begin();

    /**
     * Reverts all of the commands issued in the most recent transaction block,
     * and closes the block.
     *
     * @throws NoTransactionException if no transaction is in progress.
     */
    void rollback() throws NoTransactionException;

    /**
     * Closes all open transaction blocks, permanently applying the changes made in them.
     *
     * @throws NoTransactionException if no transaction is in progress.
     */
    void commit()  throws NoTransactionException;

    /**
     * Sets the variable <tt>key</tt> to the value <tt>value</tt>.
     * Neither keys nor values can contain spaces.
     *
     * @param key
     * @param value
     */
    void set(K key, V value);

    /**
     * the <tt>value</tt> associated with the <tt>key</tt>
     * or NULL if there's no mapping the <tt>key</tt>.
     *
     * @param key
     */
    V get(K key);

    /**
     * Removes mapping for the <tt>key</tt> if any.
     *
     * @param key
     * @return previously associated value.
     */
    V unset(K key);

    /**
     * Returns the number of keys that are currently associated with <tt>value</tt>.
     * @param value
     */
    Long countByValue(V value);
}
