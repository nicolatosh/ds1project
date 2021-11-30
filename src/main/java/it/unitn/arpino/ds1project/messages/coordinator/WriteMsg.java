package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.util.UUID;

/**
 * A message that a {@link TxnClient} uses to request a write to a {@link Coordinator}.
 */
public class WriteMsg extends TxnMessage {
    public final int key;
    public final int value;

    public WriteMsg(UUID uuid, int key, int value) {
        super(uuid);
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "WriteMsg{" +
                "uuid=" + uuid +
                ", key=" + key +
                ", value=" + value +
                '}';
    }
}
