package it.unitn.arpino.ds1project.datastore;

import java.util.HashSet;
import java.util.Set;

public class Transaction {
    private final OptimisticConcurrencyControl controller;
    private final Workspace workspace;
    private final Set<Lock> locks;

    public Transaction(OptimisticConcurrencyControl controller) {
        this.controller = controller;
        workspace = new Workspace();
        locks = new HashSet<>();
    }

    public Workspace getWorkspace() {
        return workspace;
    }

    /**
     * Reads a data item with the provided key. Updates the Workspace accordingly.
     *
     * @param key The key of the data item to read.
     * @return The value of the data item that was read.
     * @see Workspace
     */
    public int read(int key) {
        int value = controller.read(this, key);

        workspace.write(key, value);

        return value;
    }

    /**
     * Writes the provided value to the data item with the provided key. Updates the Workspace accordingly.
     *
     * @param key The key of the data item to read.
     * @see Workspace
     */
    public void write(int key, int value) {
        controller.write(this, key, value);

        workspace.write(key, value);
    }

    /**
     * Attempts to commit the the transaction. Updates the current state of the Two-phase commit (2PC)
     * protocol accordingly.
     */
    public boolean prepare() {
        return controller.prepare(this);
    }

    /**
     * Commits the transaction and updates the current state of the Two-phase commit (2PC) protocol.
     */
    public void commit() {
        controller.commit(this);
    }

    /**
     * Aborts the transaction and updates the current state of the Two-phase commit (2PC) protocol.
     */
    public void abort() {
        controller.abort(this);
    }

    protected Set<Lock> getLocks() {
        return locks;
    }

    protected void addLock(Lock lock) {
        locks.add(lock);
    }
}
