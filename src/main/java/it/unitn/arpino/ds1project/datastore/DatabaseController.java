package it.unitn.arpino.ds1project.datastore;

import java.util.List;
import java.util.stream.Collectors;

public class DatabaseController implements IDatabaseController {
    private final IDatabase database;
    private final TransactionRepository transactionRepository;
    private final ILockRepository lockRepository;

    public DatabaseController(IDatabase database) {
        this.database = database;
        transactionRepository = new TransactionRepository();
        lockRepository = new LockRepository();
    }

    @Override
    public IConnection beginTransaction() {
        IConnection connection = new Connection(this);
        ITransaction transaction = new Transaction();
        transactionRepository.setPending(transaction, connection);
        return connection;
    }

    @Override
    public int read(IConnection connection, int key) {
        ITransaction transaction = transactionRepository.getPending(connection);
        IWorkspace workspace = transaction.getWorkspace();

        // If this is the first time that the transaction reads a data item with this key,
        // we need to save the data item's version, as it will be later used by the database in the
        // optimistic concurrency control when committing.
        if (!workspace.getModifiedKeys().contains(key)) {
            workspace.setVersion(key, database.getVersion(key));
            workspace.write(key, database.read(key));
        }

        return workspace.read(key);
    }

    @Override
    public void write(IConnection connection, int key, int value) {
        ITransaction transaction = transactionRepository.getPending(connection);
        IWorkspace workspace = transaction.getWorkspace();

        // If this is the first time that the transaction writes a data item with this key,
        // we need to save the data item's version, as it will be later used by the database in the
        // optimistic concurrency control when committing.
        if (!workspace.getModifiedKeys().contains(key)) {
            workspace.setVersion(key, database.getVersion(key));
        }

        workspace.write(key, value);
    }

    @Override
    public Response prepare(IConnection connection) {
        ITransaction transaction = transactionRepository.getPending(connection);

        if (!onParWithDatabase(transaction) || !acquireLocks(transaction)) {
            abort(connection);
            return Response.ABORT;
        }

        transactionRepository.setPending(transaction, connection);
        return Response.PREPARED;
    }

    @Override
    public void commit(IConnection connection) {
        ITransaction transaction = transactionRepository.getPending(connection);
        IWorkspace workspace = transaction.getWorkspace();

        // for each data item the transaction has read or written:
        workspace.getModifiedKeys().forEach(key -> {

            // write back the data item's value into the database
            database.write(key, workspace.read(key));

            // update the data item's version
            database.setVersion(key, database.getVersion(key) + 1);
        });

        // release all locks held by the transaction
        transaction.getLocks().forEach(lockRepository::release);

        // add the transaction to the list of committed ones
        transactionRepository.setCommitted(transaction);
    }

    @Override
    public void abort(IConnection connection) {
        ITransaction transaction = transactionRepository.getPending(connection);

        // release all locks held by the transaction
        transaction.getLocks().forEach(lockRepository::release);

        // add the transaction to the list of aborted ones
        transactionRepository.setAborted(transaction);

        // TODO check for workspace. DO we need to delete it?
    }

    /**
     * Verifies that the versions of the data items that the transaction has read or written are not "dated"
     * with respect to the versions of the data items in the database. This can happen if another transaction
     * which has read or written the same data items has already committed in the meanwhile.
     *
     * @return true if all versions of the transaction's data items are on par with the database's data items,
     * false otherwise.
     */
    private boolean onParWithDatabase(ITransaction transaction) {
        IWorkspace workspace = transaction.getWorkspace();

        return workspace.getModifiedKeys().stream()
                .allMatch(key -> workspace.getVersion(key).equals(database.getVersion(key)));
    }

    /**
     * Either locks all data items that the transaction has read or written, or none.
     *
     * @return true if all data items have been locked, false if none of the data items have been locked.
     */
    private boolean acquireLocks(ITransaction transaction) {
        IWorkspace workspace = transaction.getWorkspace();

        List<Lock> locks = workspace.getModifiedKeys().stream()
                .map(lockRepository::getLock)
                .collect(Collectors.toList());

        if (locks.stream().allMatch(Lock::lock)) {
            locks.forEach(transaction.getLocks()::add);
            return true;
        }

        locks.forEach(Lock::unlock);
        return false;
    }
}
