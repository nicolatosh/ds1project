package it.unitn.arpino.ds1project.datastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Workspace {
    /**
     * The values of the data items that the transaction has read or wrote so far.
     *
     * @see Transaction
     */
    private final Map<Integer, Integer> values;

    public Workspace() {
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

    @Override
    public String toString() {
        return values.entrySet().stream()
                .sorted()
                .map(entry -> entry.getKey() + ": " + entry.getValue())
                .collect(Collectors.joining(", "));
    }
}
