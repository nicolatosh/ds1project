package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;

/**
 * A message that a {@link TxnClient} sends to itself to signal the expiration of a timer.
 */
public class TxnAcceptTimeoutMsg extends Message {
    public TxnAcceptTimeoutMsg() {
        super(null);
    }

    @Override
    public Type getType() {
        return Type.Setup;
    }
}
