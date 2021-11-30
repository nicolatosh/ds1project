package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.UUID;

/**
 * A message that a {@link Coordinator} sends to a {@link Server} requesting to vote whether to commit or abort a
 * transaction.
 */
public class VoteRequest extends TxnMessage {
    public VoteRequest(UUID uuid) {
        super(uuid);
    }

    @Override
    public String toString() {
        return "VoteRequest{" +
                "uuid=" + uuid +
                '}';
    }
}
