package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.MessageType;
import it.unitn.arpino.ds1project.twopc.Vote;

import java.io.Serializable;

public class VoteResponse implements MessageType, Serializable {
    public final Vote vote;

    public VoteResponse(Vote vote) {
        this.vote = vote;
    }

    @Override
    public TYPE getType() {
        return TYPE.TwoPC;
    }
}
