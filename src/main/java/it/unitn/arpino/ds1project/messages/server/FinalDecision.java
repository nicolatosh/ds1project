package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.Objects;
import java.util.UUID;

/**
 * A message that a {@link Coordinator} sends to a {@link Server} (or a Server to another Server, if executing the
 * termination protocol), representing the decision that the Coordinator has taken about whether to commit or abort the
 * transaction.
 */
public class FinalDecision extends Message {
    public enum Decision {
        GLOBAL_COMMIT,
        GLOBAL_ABORT
    }

    public final Decision decision;
    public boolean clientAbort;

    public FinalDecision(UUID uuid, Decision decision) {
        super(uuid);
        this.decision = decision;
    }

    public FinalDecision(UUID uuid, Decision decision, boolean clientAbort) {
        this(uuid, decision);
        this.clientAbort = clientAbort;
    }

    @Override
    public Type getType() {
        return Type.Internal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FinalDecision)) return false;
        if (!super.equals(o)) return false;
        FinalDecision decision1 = (FinalDecision) o;
        return clientAbort == decision1.clientAbort && decision == decision1.decision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), decision, clientAbort);
    }
}
