package it.unitn.arpino.ds1project.datastore.database;

import it.unitn.arpino.ds1project.datastore.controller.DatabaseController;
import it.unitn.arpino.ds1project.datastore.controller.IDatabaseController;

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
    private final Map<Integer, Integer> keyValues;

    private IDatabase database;
    private IDatabaseController controller;

    private DatabaseBuilder() {
        lowerKey = DEFAULT_LOWER_KEY;
        upperKey = DEFAULT_UPPER_KEY;
        value = DEFAULT_DATA_VALUE;
        keyValues = new HashMap<>();
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


    public DatabaseBuilder create() {
        IntStream.rangeClosed(lowerKey, upperKey).forEach(key -> keyValues.put(key, value));

        database = new Database(keyValues);

        controller = new DatabaseController(database);

        return this;
    }

    public IDatabase getDatabase() {
        return database;
    }

    public IDatabaseController getController() {
        return controller;
    }
}
