package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;

/**
 * A message that a client sends to a coordinator to begin a transaction (TXN)
 *
 * @see TxnClient
 */
public class TxnBeginMsg extends Message {
    @Override
    public Message.TYPE getType() {
        return Message.TYPE.TxnControl;
    }
}
