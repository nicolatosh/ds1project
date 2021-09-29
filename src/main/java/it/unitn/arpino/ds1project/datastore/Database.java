package it.unitn.arpino.ds1project.datastore;

import java.util.HashMap;
import java.util.Map;

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
        return new Transaction(this);
    }

    protected boolean prepare(Transaction transaction) {
        return concurrencyControl.prepare(transaction);
    }

    protected void commit(Transaction transaction) {
        concurrencyControl.commit(transaction);
    }

    public void abort(Transaction transaction) {
        concurrencyControl.abort(transaction);
    }

    /* Protected methods */

    protected int read(int key) {
        return values.get(key);
    }

    /**
     * Updates the data item value and version at the same time.
     *
     * @param key   The key of the data item to update
     * @param value The value to write
     */
    protected void write(int key, int value) {
        values.put(key, value);
        versions.merge(key, 1, Integer::sum);
    }

    protected int version(int key) {
        return versions.get(key);
    }
}
