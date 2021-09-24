package it.unitn.arpino.ds1project.nodes;

import java.util.HashMap;
import java.util.Map;

/**
 * This class represents the database that Server should use.
 * Piece of data is a triplet that contains Key, Version and Value.
 */
public class DataItem {

    private final static Integer DATA_CAPACITY = 10;
    private final Integer serverId;
    private final Map<Integer, Pair<Integer, Integer>> data;

    public DataItem(Integer serverId) {
        this.data = new HashMap<>(DATA_CAPACITY);
        this.serverId = serverId;
        this.dataInitializer();
    }

    /*-- Pair class */

    public static class Pair<X, Y> {
        private X x;
        private Y y;

        private Pair(X x, Y y) {
            this.x = x;
            this.y = y;
        }
    }

    /*-- Utility methods */

    /**
     * Initialize data item with key, version, values
     * according to server id
     */
    private void dataInitializer() {
        for (int i = 1; i <= DATA_CAPACITY; i++) {
            this.data.put(serverId * DATA_CAPACITY, new Pair<>(0, 100));
        }
    }

    public Map<Integer, Pair<Integer, Integer>> getData() {
        return data;
    }
}
