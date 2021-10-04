package it.unitn.arpino.ds1project.datastore;

public class Transaction {
    private final Database database;
    private final Workspace workspace;

    public Transaction(Database database) {
        this.database = database;
        workspace = new Workspace();
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
        // If this is the first time that the transaction reads a data item with this key,
        // we need to save the data item's version, as it will be later used by the database in the
        // optimistic concurrency control when committing.
        if (!workspace.getKeys().contains(key)) {
            // Todo add key
            workspace.setVersion(key, database.version(key));
        }

        workspace.write(key, database.read(key));

        return workspace.read(key);
    }

    /**
     * Writes the provided value to the data item with the provided key. Updates the Workspace accordingly.
     *
     * @param key The key of the data item to read.
     * @see Workspace
     */
    public void write(int key, int value) {
        // If this is the first time that the transaction reads a data item with this key,
        // we need to save the data item's version, as it will be later used by the database in the
        // optimistic concurrency control when committing.
        if (!workspace.getKeys().contains(key)) {
            workspace.setVersion(key, database.version(key));
        }

        workspace.write(key, value);
    }

    /**
     * Attempts to commit the the transaction. Updates the current state of the Two-phase commit (2PC)
     * protocol accordingly.
     */
    public boolean prepare() {
        return database.prepare(this);
    }

    /**
     * Commits the transaction and updates the current state of the Two-phase commit (2PC) protocol.
     */
    public void commit() {
        database.commit(this);
    }

    /**
     * Aborts the transaction and updates the current state of the Two-phase commit (2PC) protocol.
     */
    public void abort() {
        database.abort(this);
    }
}
