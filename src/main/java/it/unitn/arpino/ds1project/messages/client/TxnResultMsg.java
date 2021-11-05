package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.util.UUID;

/**
 * A message that a {@link Coordinator} sends to a {@link TxnClient} indicating the overall outcome of the transaction.
 */
public class TxnResultMsg extends Message {
    public final boolean commit;

    public TxnResultMsg(UUID uuid, boolean commit) {
        super(uuid);
        this.commit = commit;
    }

    @Override
    public Type getType() {
        return Type.Conversational;
    }
}
