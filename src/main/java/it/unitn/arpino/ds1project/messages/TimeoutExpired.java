package it.unitn.arpino.ds1project.messages;

import it.unitn.arpino.ds1project.nodes.DataStoreNode;

import java.util.UUID;

/**
 * A message that a {@link DataStoreNode} can send to itself to signal the expiration of a timer.
 */
public class TimeoutExpired extends Message {
    public TimeoutExpired(UUID uuid) {
        super(uuid);
    }

    @Override
    public Type getType() {
        return Type.Internal;
    }
}
