package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.util.UUID;

/**
 * A message that a {@link TxnClient} uses to request a read to a {@link Coordinator}.
 */
public class ReadMsg extends Message {
    public final int key;

    public ReadMsg(UUID uuid, int key) {
        super(uuid);
        this.key = key;
    }

    @Override
    public Type getType() {
        return Type.Conversational;
    }
}
