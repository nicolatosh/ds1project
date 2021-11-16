package it.unitn.arpino.ds1project.messages;

import akka.dispatch.ControlMessage;

public class Resume extends Message implements ControlMessage {
    public Resume() {
        super(null);
    }

    @Override
    public Type getType() {
        return Type.Setup;
    }
}
