package com.mborodin.thumbtack.simpledb;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class SimpleDB<K, V> {
    public static SimpleDB INSTANCE = new SimpleDB();

    private Map<K, V> workingMemory = new HashMap<K, V>();
    private Deque<Map<K, V>> transactions = new ArrayDeque<Map<K, V>>();

    private SimpleDB() {}

}
