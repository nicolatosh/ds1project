package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.Typed;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.nodes.server.Vote;

import java.io.Serializable;
import java.util.UUID;

/**
 * A message that a server sends to a coordinator in response of its request to vote, containing the vote itself.
 *
 * @see Server
 * @see Coordinator
 * @see Vote
 */
public class VoteResponse implements Typed, Transactional, Serializable {
    private final UUID uuid;

    /**
     * The vote of the server.
     *
     * @see Vote
     * @see Server
     */
    public final Vote vote;


    public VoteResponse(UUID uuid, Vote vote) {
        this.uuid = uuid;
        this.vote = vote;
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
