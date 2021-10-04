package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.Typed;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.io.Serializable;
import java.util.UUID;

/**
 * A message that a coordinator sends to a server requesting to vote whether a transaction should be
 * committed or aborted.
 *
 * @see Coordinator
 * @see Server
 */
public class VoteRequest implements Typed, Transactional, Serializable {
    private final UUID uuid;

    public VoteRequest(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public TYPE getType() {
        return TYPE.TwoPC;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}
