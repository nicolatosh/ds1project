package it.unitn.arpino.ds1project.twopc.messages;

import it.unitn.arpino.ds1project.transaction.Txn;

public class DecisionRequest extends AbstractTwoPcMessage {
    public DecisionRequest(Txn txn) {
        super(txn);
    }
}
