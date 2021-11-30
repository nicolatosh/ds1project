package it.unitn.arpino.ds1project.messages;

import akka.dispatch.ControlMessage;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.io.Serializable;

/**
 * A message used to signal to a {@link Coordinator} that a new {@link Server} joined the distributed Data Store, and
 * containing the keys of the data items of the server.
 */
public class JoinMessage implements ControlMessage, Serializable {
    public final int lowerKey, upperKey;

    public JoinMessage(int lowerKey, int upperKey) {
        this.lowerKey = lowerKey;
        this.upperKey = upperKey;
    }

    @Override
    public String toString() {
        return "JoinMessage{" +
                "lowerKey=" + lowerKey +
                ", upperKey=" + upperKey +
                '}';
    }
}
