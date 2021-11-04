package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.UUID;

/**
 * A message that a {@link Server} sends to a {@link Coordinator} upon its request to vote, containing the {@link Vote}
 * itself.
 */
public class VoteResponse extends Message implements Transactional {
    /**
     * Vote that the {@link Server} casts upon a {@link Coordinator}'s {@link VoteRequest}.
     */
    public enum Vote {
        /**
         * The server has successfully prepared the transaction. The transaction is ready to be committed.
         */
        YES,
        /**
         * The server could not prepare the transaction. The transaction has been automatically aborted.
         */
        NO
    }

    private final UUID uuid;

    public final Vote vote;


    public VoteResponse(UUID uuid, Vote vote) {
        this.uuid = uuid;
        this.vote = vote;
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
