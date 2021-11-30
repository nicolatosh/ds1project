package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.Objects;
import java.util.UUID;

/**
 * A message that a {@link Server} sends to a {@link Coordinator} with the result of a {@link ReadRequest}.
 */
public class ReadResult extends TxnMessage {
    public final int key;
    public final int value;

    public ReadResult(UUID uuid, int key, int value) {
        super(uuid);
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReadResult)) return false;
        if (!super.equals(o)) return false;
        ReadResult result = (ReadResult) o;
        return key == result.key && value == result.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), key, value);
    }

    @Override
    public String toString() {
        return "ReadResult{" +
                "uuid=" + uuid +
                ", key=" + key +
                ", value=" + value +
                '}';
    }
}
