package it.unitn.arpino.ds1project.transaction.messages;

import java.io.Serializable;

/**
 * Message that the client sends to a coordinator to begin the TXN
 */
public class TxnBeginMsg implements Serializable {
    public final int clientId;

    public TxnBeginMsg(int clientId) {
        this.clientId = clientId;
    }
}
