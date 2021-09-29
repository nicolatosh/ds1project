package it.unitn.arpino.ds1project.datastore;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransactionRepository {
    private List<Transaction> committed;
    private List<Transaction> aborted;
    private Map<Transaction, Set<Lock>> pending;

    public List<Transaction> getCommitted() {
        return committed;
    }

    public void setCommitted(Transaction transaction) {
        pending.remove(transaction);
        committed.add(transaction);
    }

    public Set<Lock> getLocks(Transaction transaction) {
        return pending.get(transaction);
    }

    public void setPending(Transaction transaction) {
        pending.put(transaction, new HashSet<>());
    }

    public void addLocks(Transaction transaction, Set<Lock> locks) {
        pending.get(transaction).addAll(locks);
    }

    public void setAborted(Transaction transaction) {
        pending.remove(transaction);
        aborted.add(transaction);
    }
}
