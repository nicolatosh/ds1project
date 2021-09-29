package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Typed;

import java.io.Serializable;

/**
 * The client may timeout waiting for TXN begin confirmation (TxnAcceptMsg)
 */
public class TxnAcceptTimeoutMsg implements Typed, Serializable {
    @Override
    public TYPE getType() {
        return TYPE.NodeControl;
    }
}
