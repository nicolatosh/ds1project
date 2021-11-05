package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

/**
 * A message that a {@link TxnClient} uses to open a transaction with a {@link Coordinator}.
 */
public class TxnBeginMsg extends Message {
    public TxnBeginMsg() {
        super(null);
    }

    @Override
    public Type getType() {
        return Type.Conversational;
    }
}
