package it.unitn.arpino.ds1project.datastore;

public interface IDatabase {
    Integer read(int key);

    void write(int key, int value);

    Integer getVersion(int key);

    void setVersion(int key, int version);
}
