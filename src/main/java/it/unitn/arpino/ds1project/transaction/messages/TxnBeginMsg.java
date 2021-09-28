package it.unitn.arpino.ds1project.transaction.messages;

/**
 * Message that the client sends to a coordinator to begin the TXN
 */
public class TxnBeginMsg extends AbstractTxnMessage {
    public final int clientId;

    public TxnBeginMsg(int clientId) {
        this.clientId = clientId;
    }
}
