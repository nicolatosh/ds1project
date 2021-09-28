package it.unitn.arpino.ds1project.twopc.messages;

import it.unitn.arpino.ds1project.transaction.Txn;

public class VoteRequest extends AbstractTwoPcMessage {
    public VoteRequest(Txn txn) {
        super(txn);
    }
}
