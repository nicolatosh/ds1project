package it.unitn.arpino.ds1project.datastore;

public interface ITransaction {
    IWorkspace getWorkspace();

    /**
     * Either locks all data items that the transaction has read or written, or none.
     *
     * @return true if all data items have been locked, false if none of the data items have been locked.
     */
    boolean acquireLocks();

    void releaseLocks();
}
