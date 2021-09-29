package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.Typed;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.coordinator.Decision;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.io.Serializable;
import java.util.UUID;

/**
 * A message that a coordinator sends to a server containing its final decision on
 * committing or aborting the transaction.
 *
 * @see Coordinator
 * @see Server
 * @see Decision
 */
public class FinalDecision implements Typed, Transactional, Serializable {
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
    public TYPE getType() {
        return TYPE.TwoPC;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}
