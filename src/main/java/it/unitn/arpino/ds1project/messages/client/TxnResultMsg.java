package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.MessageType;

import java.io.Serializable;

/**
 * Message from the coordinator to the client with the outcome of the TXN
 */
public class TxnResultMsg implements MessageType, Serializable {
    public final Boolean commit;

    /**
     * @param commit If false, the transaction was aborted
     */
    public TxnResultMsg(boolean commit) {
        this.commit = commit;
    }

    @Override
    public TYPE getType() {
        return TYPE.TxnControl;
    }
}
