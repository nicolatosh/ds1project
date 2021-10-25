package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.Message;

/**
 * The client may timeout waiting for TXN begin confirmation (TxnAcceptMsg)
 */
public class TxnAcceptTimeoutMsg extends Message {
    @Override
    public Message.TYPE getType() {
        return Message.TYPE.NodeControl;
    }
}
