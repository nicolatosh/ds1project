package it.unitn.arpino.ds1project.datastore;

public interface IDatabaseController {
    enum Response {
        PREPARED,
        ABORT
    }

    IConnection beginTransaction();

    int read(IConnection connection, int key);

    void write(IConnection connection, int key, int value);

    Response prepare(IConnection connection);

    void commit(IConnection connection);

    void abort(IConnection connection);
}
