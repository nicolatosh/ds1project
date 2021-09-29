package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.MessageType;

import java.io.Serializable;

/**
 * Reply from the coordinator receiving TxnBeginMsg
 */
public class TxnAcceptMsg implements MessageType, Serializable {
    @Override
    public TYPE getType() {
        return TYPE.TxnControl;
    }
}
