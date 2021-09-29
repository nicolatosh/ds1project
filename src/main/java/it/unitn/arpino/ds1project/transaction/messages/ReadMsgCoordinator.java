package it.unitn.arpino.ds1project.transaction.messages;

import it.unitn.arpino.ds1project.transaction.Txn;

import java.io.Serializable;

public class ReadMsgCoordinator implements Serializable {

    public final Txn txn;
    public final int coordinatorId;
    public final int key;

    public ReadMsgCoordinator(Txn txn, int coordinatorId, int key) {
        this.coordinatorId = coordinatorId;
        this.txn = txn;
        this.key = key;
    }
}
