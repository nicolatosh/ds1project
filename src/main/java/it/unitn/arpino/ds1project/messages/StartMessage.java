package it.unitn.arpino.ds1project.messages;

import akka.dispatch.ControlMessage;

public class StartMessage implements ControlMessage {
    @Override
    public String toString() {
        return "StartMessage";
    }
}
