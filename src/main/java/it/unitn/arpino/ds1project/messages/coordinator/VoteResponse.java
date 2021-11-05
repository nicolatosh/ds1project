package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.Objects;
import java.util.UUID;

/**
 * A message that a {@link Server} sends to a {@link Coordinator} in response of a {@link VoteRequest}.
 */
public class VoteResponse extends Message {
    public enum Vote {
        /**
         * The Server has successfully prepared the transaction. The transaction is ready to be committed.
         */
        YES,
        /**
         * The Server could not prepare the transaction. The transaction has been automatically aborted.
         */
        NO
    }

    public final Vote vote;

    public VoteResponse(UUID uuid, Vote vote) {
        super(uuid);
        this.vote = vote;
    }

    @Override
    public Type getType() {
        return Type.Internal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VoteResponse)) return false;
        if (!super.equals(o)) return false;
        VoteResponse that = (VoteResponse) o;
        return vote == that.vote;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), vote);
    }
}
