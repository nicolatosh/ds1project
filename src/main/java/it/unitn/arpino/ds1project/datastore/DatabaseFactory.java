package it.unitn.arpino.ds1project.datastore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DatabaseFactory {

    private static final int KEY_RANGE = 10;
    private static final int KEY_RANGE_UPPER_BOUND = 9;
    private static final int DEFAULT_DATA_VALUE = 100;

    Map<Integer, Integer> keyvalues;

    public DatabaseFactory(int serverId) {
        this.keyvalues = new HashMap<>();

        // Generating keys according to formula "10i : 10i + 9"
        List<Integer> keys = IntStream.rangeClosed(KEY_RANGE * serverId, KEY_RANGE * serverId + KEY_RANGE_UPPER_BOUND).boxed().collect(Collectors.toList());

        System.out.println("BBB " + keys);
        keys.forEach(k -> {
            keyvalues.put(k, DEFAULT_DATA_VALUE);
        });
    }

    public IDatabaseController getController() {
        IDatabase database = new Database();
        database.initializeDb(keyvalues);
        return new DatabaseController(database);
    }
}
