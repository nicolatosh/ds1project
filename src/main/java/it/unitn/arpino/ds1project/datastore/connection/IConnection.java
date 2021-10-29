package it.unitn.arpino.ds1project.datastore.connection;

import it.unitn.arpino.ds1project.datastore.controller.IDatabaseController;

public interface IConnection {
    int read(int key);

    void write(int key, int value);

    IDatabaseController.Response prepare();

    void commit();

    void abort();
}
