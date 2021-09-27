package it.unitn.arpino.ds1project.datastore;

import java.util.HashMap;
import java.util.Map;

public class Workspace {
    // Todo: add transaction ID
    int transactionId;
    private final Map<Integer, DataItem> data;

    public Workspace(int transactionId) {
        this.transactionId = transactionId;
        this.data = new HashMap<>();
    }

    public void addDataItem(DataItem item) {
        this.data.put(item.getKey(), item);
    }

    private DataItem read(int key) {
        return data.get(key);
    }

    private void write(int key, int value) {
        int version = data.get(key).getVersion();
        DataItem item = new DataItem(key, version + 1, value);
        this.data.put(key, item);
    }
}
