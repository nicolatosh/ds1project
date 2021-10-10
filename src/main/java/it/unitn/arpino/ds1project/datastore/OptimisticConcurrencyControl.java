package it.unitn.arpino.ds1project.datastore;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OptimisticConcurrencyControl {
    private final Database database;

    private final TransactionRepository transactionRepository;

    private final LockRepository lockRepository;

    private final Map<Transaction, Map<Integer, Integer>> versions;

    public OptimisticConcurrencyControl(Database database) {
        this.database = database;
        transactionRepository = new TransactionRepository();
        lockRepository = new LockRepository();
        versions = new HashMap<>();
    }

    public boolean prepare(Transaction transaction) {
        if (!onParWithDatabase(transaction)) {
            return false;
        }
        if (!acquireLocks(transaction)) {
            return false;
        }
        transactionRepository.setPending(transaction);
        return true;
    }

    public void commit(Transaction transaction) {
        Workspace workspace = transaction.getWorkspace();

        // for each data item the transaction has read or written:
        workspace.getKeys().forEach(key -> {

            // write back the data item's value into the database
            database.write(key, workspace.read(key));

            // update the data item's version
            database.setVersion(key, database.version(key) + 1);
        });

        // release all locks held by the transaction
        transaction.getLocks().forEach(lockRepository::release);

        // add the transaction to the list of committed ones
        transactionRepository.setCommitted(transaction);
    }

    public void abort(Transaction transaction) {
        // release all locks held by the transaction
        transaction.getLocks().forEach(lockRepository::release);

        // add the transaction to the list of aborted ones
        transactionRepository.setAborted(transaction);
    }

    /**
     * Verifies that the versions of the data items that the transaction has read or written are not "dated"
     * with respect to the versions of the data items in the database. This can happen if another transaction
     * which has read or written the same data items has already committed in the meanwhile.
     *
     * @return true if all versions of the transaction's data items are on par with the database's data items,
     * false otherwise.
     */
    private boolean onParWithDatabase(Transaction transaction) {
        Workspace workspace = transaction.getWorkspace();

        return workspace.getKeys().stream()
                .allMatch(key -> versions.get(transaction).get(key) == database.version(key));
    }

    /**
     * Either locks all data items that the transaction has read or written, or none.
     *
     * @return true if all data items have been locked, false if none of the data items have been locked.
     */
    private boolean acquireLocks(Transaction transaction) {
        Workspace workspace = transaction.getWorkspace();

        Set<Lock> locks = workspace.getKeys().stream()
                .map(lockRepository::getLock)
                .collect(Collectors.toSet());

        if (locks.stream().allMatch(Lock::lock)) {
            locks.forEach(transaction::addLock);
            return true;
        }

        locks.forEach(Lock::unlock);
        return false;
    }

    protected int read(Transaction transaction, int key) {
        // If this is the first time that the transaction reads a data item with this key,
        // we need to save the data item's version, as it will be later used by the database in the
        // optimistic concurrency control when committing.
        versions.putIfAbsent(transaction, new HashMap<>()).putIfAbsent(key, database.version(key));
        return database.read(key);
    }

    protected void write(Transaction transaction, int key, int value) {
        // If this is the first time that the transaction writes a data item with this key,
        // we need to save the data item's version, as it will be later used by the database in the
        // optimistic concurrency control when committing.
        versions.putIfAbsent(transaction, new HashMap<>()).putIfAbsent(key, database.version(key));
        database.write(key, value);
    }
}
