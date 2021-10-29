package it.unitn.arpino.ds1project.datastore.transaction;

import it.unitn.arpino.ds1project.datastore.connection.IConnection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionRepository implements ITransactionRepository {
    private final List<ITransaction> committed;
    private final List<ITransaction> aborted;
    private final Map<ITransaction, IConnection> pending;

    public TransactionRepository() {
        committed = new ArrayList<>();
        aborted = new ArrayList<>();
        pending = new HashMap<>();
    }

    @Override
    public void setCommitted(ITransaction transaction) {
        pending.remove(transaction);
        committed.add(transaction);
    }

    @Override
    public void setPending(ITransaction transaction, IConnection connection) {
        pending.put(transaction, connection);
    }

    @Override
    public void setAborted(ITransaction transaction) {
        pending.remove(transaction);
        aborted.add(transaction);
    }

    @Override
    public List<ITransaction> getCommitted() {
        return committed;
    }

    @Override
    public List<ITransaction> getAborted() {
        return aborted;
    }

    @Override
    public Map<ITransaction, IConnection> getPending() {
        return pending;
    }

    @Override
    public ITransaction getPending(IConnection connection) {
        return pending.entrySet().stream()
                .filter(entry -> entry.getValue() == connection)
                .map(Map.Entry::getKey).findFirst()
                .get();
    }
}
