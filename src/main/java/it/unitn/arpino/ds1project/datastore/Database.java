package it.unitn.arpino.ds1project.datastore;

import java.util.HashMap;
import java.util.Map;

/**
 * The Database that a Server uses to store data items.
 */
public class Database {

    private final static int DATA_CAPACITY = 10;
    private final int serverId;
    private final Map<Integer, DataItem> data;

    public Database(int serverId) {
        this.serverId = serverId;

        this.data = new HashMap<>(DATA_CAPACITY);
        for (int i = 0; i < DATA_CAPACITY; i++) {
            int key = 10 * serverId + i;
            this.data.put(key, new DataItem(key, 0, 100));
        }
    }

    public DataItem getDataItemByKey(int key) {
        return data.get(key);
    }
}
