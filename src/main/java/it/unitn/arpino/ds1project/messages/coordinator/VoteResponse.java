package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.nodes.server.Vote;

import java.util.UUID;

/**
 * A message that a server sends to a coordinator in response of its request to vote, containing the vote itself.
 *
 * @see Server
 * @see Coordinator
 * @see Vote
 */
public class VoteResponse extends Message implements Transactional {
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
    public Message.TYPE getType() {
        return Message.TYPE.TwoPC;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}
