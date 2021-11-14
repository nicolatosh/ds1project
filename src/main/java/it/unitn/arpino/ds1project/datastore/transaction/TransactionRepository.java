package it.unitn.arpino.ds1project.datastore.transaction;

import it.unitn.arpino.ds1project.datastore.connection.IConnection;

import java.util.*;

public class TransactionRepository implements ITransactionRepository {
    private final List<ITransaction> committed;
    private final List<ITransaction> aborted;
    private final Map<ITransaction, IConnection> pending;

    public TransactionRepository() {
        committed = Collections.synchronizedList(new ArrayList<>());
        aborted = Collections.synchronizedList(new ArrayList<>());
        pending = Collections.synchronizedMap(new HashMap<>());
    }

    @Override
    public synchronized void setCommitted(ITransaction transaction) {
        pending.remove(transaction);
        committed.add(transaction);
    }

    @Override
    public synchronized void setPending(ITransaction transaction, IConnection connection) {
        pending.put(transaction, connection);
    }

    @Override
    public synchronized void setAborted(ITransaction transaction) {
        pending.remove(transaction);
        aborted.add(transaction);
    }

    @Override
    public synchronized List<ITransaction> getCommitted() {
        return committed;
    }

    @Override
    public synchronized List<ITransaction> getAborted() {
        return aborted;
    }

    @Override
    public synchronized Map<ITransaction, IConnection> getPending() {
        return pending;
    }

    @Override
    public synchronized ITransaction getPending(IConnection connection) {
        return pending.entrySet().stream()
                .filter(entry -> entry.getValue().equals(connection))
                .map(Map.Entry::getKey).findFirst()
                .get();
    }
}
