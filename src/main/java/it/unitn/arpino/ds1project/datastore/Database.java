package it.unitn.arpino.ds1project.datastore;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

/**
 * A general-purpose database, providing Read and Write methods and support for transactional operations.
 * Since Akka is synchronous, if the database methods are called, race conditions do not happen.
 */
public class Database {
    /**
     * Associates a data item's key with the data item's version.
     */
    private final Map<Integer, Integer> versions;

    /**
     * Associates a data item's key with the data item's value.
     */
    private final Map<Integer, Integer> values;

    private final OptimisticConcurrencyControl concurrencyControl;

    public Database() {
        versions = new HashMap<>();
        values = new HashMap<>();
        concurrencyControl = new OptimisticConcurrencyControl(this);
    }

    /* Public methods */

    public Transaction beginTransaction() {
        return new Transaction(concurrencyControl);
    }

    /* Protected methods */

    protected int read(int key) {
        return values.get(key);
    }

    protected void write(int key, int value) {
        values.put(key, value);
    }

    protected int version(int key) {
        return versions.get(key);
    }

    protected void setVersion(int key, int version) {
        versions.put(key, version);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Key | Ver | Val\n");
        values.keySet().stream()
                .sorted()
                .forEach(key -> {
                    sb.append(" ").append(key).append("   ");
                    sb.append("  ").append(versions.get(key)).append(" ");
                    sb.append("  ").append(values.get(key)).append("\n");
                });
        return sb.toString();
    }
}
