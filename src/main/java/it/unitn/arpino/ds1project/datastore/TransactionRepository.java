package it.unitn.arpino.ds1project.datastore;

import java.util.ArrayList;
import java.util.List;

public class TransactionRepository {
    private List<Transaction> committed;
    private List<Transaction> aborted;
    private List<Transaction> pending;

    public TransactionRepository() {
        committed = new ArrayList<>();
        aborted = new ArrayList<>();
        pending = new ArrayList<>();
    }

    public List<Transaction> getCommitted() {
        return committed;
    }

    public void setCommitted(Transaction transaction) {
        pending.remove(transaction);
        committed.add(transaction);
    }

    public void setPending(Transaction transaction) {
        pending.add(transaction);
    }

    public void setAborted(Transaction transaction) {
        pending.remove(transaction);
        aborted.add(transaction);
    }
}
