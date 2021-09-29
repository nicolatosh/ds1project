package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.MessageType;

import java.io.Serializable;

/**
 * Message that the client sends to a coordinator to end the TXN;
 * it may ask for commit or abort
 */
public class TxnEndMsg implements MessageType, Serializable {
    public final boolean commit;

    /**
     * @param commit If false, the transaction should abort
     */
    public TxnEndMsg(boolean commit) {
        this.commit = commit;
    }

    @Override
    public TYPE getType() {
        return TYPE.TxnControl;
    }
}
