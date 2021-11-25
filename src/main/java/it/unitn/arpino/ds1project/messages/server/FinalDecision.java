package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.Objects;
import java.util.UUID;

/**
 * A message that a {@link Coordinator} sends to a {@link Server},
 * representing the decision that the Coordinator has taken on committing or aborting the transaction.
 */
public class FinalDecision extends TxnMessage {
    public enum Decision {
        GLOBAL_COMMIT,
        GLOBAL_ABORT
    }

    public final Decision decision;

    public FinalDecision(UUID uuid, Decision decision) {
        super(uuid);
        this.decision = decision;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FinalDecision)) return false;
        if (!super.equals(o)) return false;
        FinalDecision decision1 = (FinalDecision) o;
        return decision == decision1.decision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), decision);
    }
}
