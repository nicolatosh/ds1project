package it.unitn.arpino.ds1project.datastore;

import java.util.List;
import java.util.Map;

public interface ITransactionRepository {
    List<ITransaction> getCommitted();

    void setCommitted(ITransaction transaction);

    List<ITransaction> getAborted();

    void setAborted(ITransaction transaction);

    Map<ITransaction, IConnection> getPending();

    ITransaction getPending(IConnection connection);

    void setPending(ITransaction transaction, IConnection connection);
}
