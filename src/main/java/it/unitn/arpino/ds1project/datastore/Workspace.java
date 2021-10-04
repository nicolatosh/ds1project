package it.unitn.arpino.ds1project.datastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Workspace {
    /**
     * The versions that the data items had when the transaction had first read or wrote them.
     *
     * @see Transaction
     */
    private final Map<Integer, Integer> versions;

    /**
     * The values of the data items that the transaction has read or wrote so far.
     *
     * @see Transaction
     */
    private final Map<Integer, Integer> values;

    public Workspace() {
        versions = new HashMap<>();
        values = new HashMap<>();
    }

    public int read(int key) {
        return values.get(key);
    }

    public void write(int key, int value) {
        values.put(key, value);
    }

    public List<Integer> getKeys() {
        return new ArrayList<>(values.keySet());
    }

    public int getVersion(int key) {
        return versions.get(key);
    }

    public void setVersion(int key, int version) {
        versions.put(key, version);
    }
}
