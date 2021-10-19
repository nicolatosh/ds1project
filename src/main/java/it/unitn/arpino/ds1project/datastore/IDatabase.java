package it.unitn.arpino.ds1project.datastore;

import java.util.Map;

public interface IDatabase {

    void initializeDb(Map<Integer, Integer> keyvalues);

    Integer read(int key);

    void write(int key, int value);

    Integer getVersion(int key);

    void setVersion(int key, int version);
}
