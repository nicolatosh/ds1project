package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.UUID;

/**
 * A message that a coordinator sends to a server requesting to vote whether a transaction should be
 * committed or aborted.
 *
 * @see Coordinator
 * @see Server
 */
public class VoteRequest extends Message implements Transactional {
    private final UUID uuid;

    public VoteRequest(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public Message.TYPE getType() {
        return Message.TYPE.TwoPC;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}
