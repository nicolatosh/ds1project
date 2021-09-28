package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.MessageType;

import java.io.Serializable;

public class VoteRequest implements MessageType, Serializable {
    @Override
    public TYPE getType() {
        return TYPE.TwoPC;
    }
}
