package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.UUID;

/**
 * A message that a coordinator sends to a server containing its final decision on
 * committing or aborting the transaction.
 *
 * @see Coordinator
 * @see Server
 * @see Decision
 */
public class FinalDecision extends Message implements Transactional {
    public enum Decision {
        GLOBAL_COMMIT,
        GLOBAL_ABORT
    }

    private final UUID uuid;

    /**
     * The decision of the coordinator about committing or aborting the transaction.
     *
     * @see Coordinator
     * @see Decision
     */
    public final Decision decision;

    public FinalDecision(UUID uuid, Decision decision) {
        this.uuid = uuid;
        this.decision = decision;
    }

    @Override
    public Message.TYPE getType() {
        return Message.TYPE.TwoPC;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}
