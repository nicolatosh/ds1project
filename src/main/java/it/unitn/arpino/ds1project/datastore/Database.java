package it.unitn.arpino.ds1project.datastore;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The Database that a Server uses to store data items.
 */
public class Database {

    private final static int DATA_CAPACITY = 10;
    private final Map<Integer, DataItem> data;

    public Database(Set<Integer> keys) {
        this.data = new HashMap<>(DATA_CAPACITY);

        keys.forEach(key -> {
            DataItem item = new DataItem(key, 0, 100);
            data.put(key, item);
        });
    }

    public DataItem getDataItemByKey(int key) {
        return data.get(key);
    }
}
