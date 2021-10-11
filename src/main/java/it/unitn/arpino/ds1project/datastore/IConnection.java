package it.unitn.arpino.ds1project.datastore;

public interface IConnection {
    int read(int key);

    void write(int key, int value);

    boolean prepare();

    void commit();

    void abort();
}
