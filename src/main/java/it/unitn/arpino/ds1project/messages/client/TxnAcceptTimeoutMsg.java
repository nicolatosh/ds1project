package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.MessageType;

import java.io.Serializable;

/**
 * The client may timeout waiting for TXN begin confirmation (TxnAcceptMsg)
 */
public class TxnAcceptTimeoutMsg implements MessageType, Serializable {
    @Override
    public TYPE getType() {
        return TYPE.NodeControl;
    }
}
