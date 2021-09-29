package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Typed;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;

import java.io.Serializable;

/**
 * A message that a client sends to a coordinator to begin a transaction (TXN)
 *
 * @see TxnClient
 */
public class TxnBeginMsg implements Typed, Serializable {
    @Override
    public TYPE getType() {
        return TYPE.TxnControl;
    }
}
