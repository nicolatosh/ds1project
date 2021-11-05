package it.unitn.arpino.ds1project.messages;

import it.unitn.arpino.ds1project.nodes.AbstractNode;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Base class for a message exchanged between {@link AbstractNode}s.
 */
public abstract class Message implements Serializable {
    /**
     * Identifier of the transaction.
     */
    public final UUID uuid;

    public Message(UUID uuid) {
        this.uuid = uuid;
    }

    public enum Type {
        /**
         * Request between a {@link TxnClient} and a {@link Coordinator}.
         */
        Conversational,

        /**
         * Message between {@link DataStoreNode}s (i.e., a {@link Coordinator} and a {@link Server}).
         */
        Internal,

        /**
         * Message to set up a {@link AbstractNode} or control its behavior.
         */
        Setup
    }

    public abstract Type getType();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message)) return false;
        Message message = (Message) o;
        return Objects.equals(uuid, message.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }
}
