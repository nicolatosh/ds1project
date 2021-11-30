package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.UUID;

/**
 * A message that a {@link Server} sends to another Server requesting the {@link FinalDecision}.
 */
public class DecisionRequest extends TxnMessage {
    public DecisionRequest(UUID uuid) {
        super(uuid);
    }

    @Override
    public String toString() {
        return "DecisionRequest{" +
                "uuid=" + uuid +
                '}';
    }
}
