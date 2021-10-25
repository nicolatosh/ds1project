package it.unitn.arpino.ds1project.datastore;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class DatabaseBuilder {
    public static final int DEFAULT_LOWER_KEY = 0;
    public static final int DEFAULT_UPPER_KEY = 9;
    public static final int DEFAULT_DATA_VALUE = 100;

    private int lowerKey;
    private int upperKey;
    private int value;
    private final Map<Integer, Integer> keyvalues;

    private DatabaseBuilder() {
        lowerKey = DEFAULT_LOWER_KEY;
        upperKey = DEFAULT_UPPER_KEY;
        value = DEFAULT_DATA_VALUE;
        keyvalues = new HashMap<>();
    }

    public static DatabaseBuilder newBuilder() {
        return new DatabaseBuilder();
    }

    public DatabaseBuilder keyRange(int lowerKey, int upperKey) {
        this.lowerKey = lowerKey;
        this.upperKey = upperKey;
        return this;
    }

    public DatabaseBuilder value(int value) {
        this.value = value;
        return this;
    }


    public IDatabaseController create() {
        // Generating keys
        IntStream.rangeClosed(lowerKey, upperKey).forEach(key -> keyvalues.put(key, value));

        IDatabase database = new Database();
        database.initializeDb(keyvalues);
        return new DatabaseController(database);
    }
}
