package it.unitn.arpino.ds1project.datastore.controller;

import it.unitn.arpino.ds1project.datastore.connection.IConnection;

/**
 * Interface to a concurrency controller.
 */
public interface IDatabaseController {
    /**
     * The result of the attempt to prepare a transaction.
     */
    enum Response {
        /**
         * The attempt to prepare the transaction was successful. No other transaction that has read or written
         * a data item that this transaction has also read or written can proceed to commit.
         */
        PREPARED,
        /**
         * The attempt to prepare the transaction was successful. The transaction has been automatically aborted.
         */
        ABORT
    }

    IConnection beginTransaction();

    int read(IConnection connection, int key);

    void write(IConnection connection, int key, int value);

    Response prepare(IConnection connection);

    void commit(IConnection connection);

    void abort(IConnection connection);
}
