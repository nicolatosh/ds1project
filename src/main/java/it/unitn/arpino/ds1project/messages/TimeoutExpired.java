package it.unitn.arpino.ds1project.messages;

import it.unitn.arpino.ds1project.nodes.DataStoreNode;

import java.util.UUID;

/**
 * A message that a {@link DataStoreNode} can send to itself to signal the expiration of a timer.
 */


public class TimeoutExpired extends Message {
    public enum TimeoutType {
        /**
         * The Server did not receive the VoteRequest in time.
         */
        VOTE_REQUEST,
        /**
         * The Coordinator did not collect all VoteRequests in time.
         */
        VOTE_RESPONSE,
        /**
         * The Server did not receive the FinalDecision in time.
         */
        FINAL_DECISION
    }

    public final TimeoutType type;

    public TimeoutExpired(UUID uuid, TimeoutType type) {
        super(uuid);
        this.type = type;
    }

    @Override
    public Type getType() {
        return Type.Internal;
    }
}
