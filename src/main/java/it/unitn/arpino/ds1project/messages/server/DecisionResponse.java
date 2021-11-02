package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext;

import java.util.UUID;

public class DecisionResponse extends Message implements Transactional {
    private final UUID uuid;
    private final ServerRequestContext.TwoPhaseCommitFSM status;


    public DecisionResponse(UUID uuid, ServerRequestContext.TwoPhaseCommitFSM status) {
        this.uuid = uuid;
        this.status = status;
    }

    @Override
    public Message.TYPE getType() {
        return Message.TYPE.TwoPC;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }

    public ServerRequestContext.TwoPhaseCommitFSM getStatus() {
        return status;
    }
}
