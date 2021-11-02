package it.unitn.arpino.ds1project.messages;

import java.util.UUID;

/**
 * This is a message that the coordinator sends to itself indicating that the timeout duration for collecting
 * all VoteResponses has elapsed. Upon receiving it, the coordinator should abort the ongoing transaction.
 */
public class TimeoutExpired extends Message implements Transactional {
    private UUID uuid;

    public TimeoutExpired(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public Message.TYPE getType() {
        return TYPE.TwoPC;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}
