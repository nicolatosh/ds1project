package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.util.UUID;

/**
 * A message that a {@link Coordinator} sends to a {@link TxnClient} with the result of a {@link ReadMsg}.
 */
public class ReadResultMsg extends Message {
    public final int key;
    public final int value;

    public ReadResultMsg(UUID uuid, int key, int value) {
        super(uuid);
        this.key = key;
        this.value = value;
    }

    @Override
    public Type getType() {
        return Type.Conversational;
    }
}
