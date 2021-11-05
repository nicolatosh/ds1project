package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.UUID;

/**
 * A message that a {@link Server} sends to another Server requesting the {@link FinalDecision}.
 */
public class DecisionRequest extends Message {
    public DecisionRequest(UUID uuid) {
        super(uuid);
    }

    @Override
    public Type getType() {
        return Type.Internal;
    }
}
