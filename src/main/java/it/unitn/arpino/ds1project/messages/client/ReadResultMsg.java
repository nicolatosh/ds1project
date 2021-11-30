package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.util.Objects;
import java.util.UUID;

/**
 * A message that a {@link Coordinator} sends to a {@link TxnClient} with the result of a {@link ReadMsg}.
 */
public class ReadResultMsg extends TxnMessage {
    public final int key;
    public final int value;

    public ReadResultMsg(UUID uuid, int key, int value) {
        super(uuid);
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReadResultMsg)) return false;
        if (!super.equals(o)) return false;
        ReadResultMsg that = (ReadResultMsg) o;
        return key == that.key && value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), key, value);
    }

    @Override
    public String toString() {
        return "ReadResultMsg{" +
                "uuid=" + uuid +
                ", key=" + key +
                ", value=" + value +
                '}';
    }
}
