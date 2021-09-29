package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.MessageType;

import java.io.Serializable;

/**
 * Message that the client sends to a coordinator to begin the TXN
 */
public class TxnBeginMsg implements MessageType, Serializable {
    public TxnBeginMsg() {
    }

    @Override
    public TYPE getType() {
        return TYPE.TxnControl;
    }
}
