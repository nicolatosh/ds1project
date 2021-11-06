package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext.TwoPhaseCommitFSM;

import java.util.UUID;

/**
 * A message that a {@link Server} sends to another Server to inform him about the final decision of the transaction.
 */
public class DecisionResponse extends Message {

    private TwoPhaseCommitFSM status;

    public DecisionResponse(UUID uuid, ServerRequestContext.TwoPhaseCommitFSM status) {
        super(uuid);
        this.status = status;
    }

    @Override
    public Type getType() {
        return Type.Internal;
    }

    public TwoPhaseCommitFSM getStatus() {
        return status;
    }
}
