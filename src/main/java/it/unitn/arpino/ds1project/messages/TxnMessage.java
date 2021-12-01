package it.unitn.arpino.ds1project.messages;

import it.unitn.arpino.ds1project.nodes.DataStoreNode;

import java.io.Serializable;
import java.util.UUID;

/**
 * Base class for a transactional message exchanged between {@link DataStoreNode}s.
 */
public abstract class TxnMessage implements Serializable {
    /**
     * Identifier of the transaction.
     */
    public final UUID uuid;

    public TxnMessage(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TxnMessage)) return false;
        TxnMessage that = (TxnMessage) o;
        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }

    @Override
    public String toString() {
        return "TxnMessage{" +
                "uuid=" + uuid +
                '}';
    }
}
