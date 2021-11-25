package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.UUID;

/**
 * A message that a {@link Server} sends to another Server to inform him about the final decision of the transaction.
 */
public class DecisionResponse extends TxnMessage {
    public enum Decision {
        UNKNOWN,
        GLOBAL_COMMIT,
        GLOBAL_ABORT
    }

    public final Decision decision;

    public DecisionResponse(UUID uuid, Decision decision) {
        super(uuid);
        this.decision = decision;
    }
}
