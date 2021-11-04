package it.unitn.arpino.ds1project.datastore.connection;

import it.unitn.arpino.ds1project.datastore.controller.IDatabaseController;

public interface IConnection {
    /**
     * Reads the data item in the database with the specified key.
     *
     * @param key Key of the data item which should be read.
     * @return The read value.
     */
    int read(int key);

    /**
     * Writes a value to the data item in the database with the specified key.
     *
     * @param key   The key of the data item to write.
     * @param value The value to write into the data item.
     */
    void write(int key, int value);

    /**
     * Attempts to prepare the transaction to be committed. If the preparation fails, the transaction is automatically
     * aborted.
     */
    IDatabaseController.Response prepare();

    /**
     * Commits the transaction.
     */
    void commit();

    /**
     * Aborts the transaction.
     */
    void abort();
}
