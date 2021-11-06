package it.unitn.arpino.ds1project.messages;

import it.unitn.arpino.ds1project.nodes.DataStoreNode;

import java.util.UUID;

/**
 * A message that a {@link DataStoreNode} can send to itself to signal the expiration of a timer.
 */


public class TimeoutExpired extends Message {
    public enum TIMEOUT_TYPE {
        VOTE_REQUEST_MISSING,
        FINALDECISION_RESPONSE_MISSING,
        VOTE_RESPONSE_MISSING
    }

    private TIMEOUT_TYPE timeout_type;

    public TimeoutExpired(UUID uuid, TIMEOUT_TYPE type) {
        super(uuid);
        this.timeout_type = type;
    }

    @Override
    public Type getType() {
        return Type.Internal;
    }

    public TIMEOUT_TYPE getTimeout_type() {
        return timeout_type;
    }
}
