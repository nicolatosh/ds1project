package it.unitn.arpino.ds1project.transaction.messages;

import java.io.Serializable;

/**
 * Message that the client sends to a coordinator to end the TXN;
 * it may ask for commit or abort
 */
public class TxnEndMsg implements Serializable {
    public final int clientId;
    public final boolean commit;

    /**
     * @param clientId
     * @param commit   If false, the transaction should abort
     */
    public TxnEndMsg(int clientId, boolean commit) {
        this.clientId = clientId;
        this.commit = commit;
    }
}
