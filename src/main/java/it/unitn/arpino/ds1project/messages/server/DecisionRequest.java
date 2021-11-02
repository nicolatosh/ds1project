package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.Transactional;

import java.util.UUID;

public class DecisionRequest extends Message implements Transactional {
    private final UUID uuid;

    public DecisionRequest(UUID uuid) {
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
