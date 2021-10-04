package it.unitn.arpino.ds1project.datastore;

import java.util.Set;
import java.util.stream.Collectors;

public class OptimisticConcurrencyControl {
    private final Database database;

    private final TransactionRepository transactionRepository;

    private final LockRepository lockRepository;

    public OptimisticConcurrencyControl(Database database) {
        this.database = database;
        transactionRepository = new TransactionRepository();
        lockRepository = new LockRepository();
    }

    public boolean prepare(Transaction transaction) {
        if (!onPar(transaction)) {
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
        workspace.getKeys().forEach(key -> database.write(key, workspace.read(key)));
        transactionRepository.getLocks(transaction).forEach(lockRepository::release);
        transactionRepository.setCommitted(transaction);
    }

    public void abort(Transaction transaction) {
        transactionRepository.getLocks(transaction).forEach(lockRepository::release);
        transactionRepository.setAborted(transaction);
    }

    /**
     * Verifies whether the data items in the database have not undergone updates in the meanwhile of the transaction
     */
    private boolean onPar(Transaction transaction) {
        Workspace workspace = transaction.getWorkspace();

        return workspace.getKeys().stream()
                .allMatch(key -> workspace.getVersion(key) == database.version(key));
    }

    /**
     * Attempts to lock all the data items that the transaction has read or written
     */
    private boolean acquireLocks(Transaction transaction) {
        Workspace workspace = transaction.getWorkspace();

        Set<Lock> locks = workspace.getKeys().stream()
                .map(lockRepository::getLock)
                .collect(Collectors.toSet());

        if (locks.stream().allMatch(Lock::lock)) {
            transactionRepository.addLocks(transaction, locks);
            return true;
        }

        locks.forEach(Lock::unlock);
        // Todo
        return false;
    }
}
