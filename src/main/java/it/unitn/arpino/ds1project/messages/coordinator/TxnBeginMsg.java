package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

/**
 * A message that a {@link TxnClient} uses to open a transaction with a {@link Coordinator}.
 */
public class TxnBeginMsg extends TxnMessage {
    public TxnBeginMsg() {
        super(null);
    }

    @Override
    public String toString() {
        return "TxnBeginMsg";
    }
}
