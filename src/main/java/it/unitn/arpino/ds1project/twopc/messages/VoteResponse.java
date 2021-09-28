package it.unitn.arpino.ds1project.twopc.messages;

import it.unitn.arpino.ds1project.transaction.Txn;
import it.unitn.arpino.ds1project.twopc.Vote;

public class VoteResponse extends AbstractTwoPcMessage {
    public final Vote vote;

    public VoteResponse(Txn txn, Vote vote) {
        super(txn);
        this.vote = vote;
    }
}
