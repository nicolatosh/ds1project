package it.unitn.arpino.ds1project.twopc.messages;

import it.unitn.arpino.ds1project.transaction.Txn;

import java.io.Serializable;

public class AbstractTwoPcMessage implements Serializable {
    public final Txn txn;

    public AbstractTwoPcMessage(Txn txn) {
        this.txn = txn;
    }
}
