package it.unitn.arpino.ds1project.twopc.messages;

import it.unitn.arpino.ds1project.twopc.Vote;

import java.io.Serializable;

public class VoteResponse implements Serializable {
    public final Vote vote;

    public VoteResponse(Vote vote) {
        this.vote = vote;
    }
}
