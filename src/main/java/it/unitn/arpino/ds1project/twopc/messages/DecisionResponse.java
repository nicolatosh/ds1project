package it.unitn.arpino.ds1project.twopc.messages;

import it.unitn.arpino.ds1project.transaction.Txn;
import it.unitn.arpino.ds1project.twopc.Decision;

public class DecisionResponse extends AbstractTwoPcMessage {
    public final Decision decision;

    public DecisionResponse(Txn txn, Decision decision) {
        super(txn);
        this.decision = decision;
    }
}
