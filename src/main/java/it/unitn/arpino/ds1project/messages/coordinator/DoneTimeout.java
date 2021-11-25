package it.unitn.arpino.ds1project.messages.coordinator;

import akka.dispatch.ControlMessage;
import it.unitn.arpino.ds1project.messages.TxnMessage;

import java.util.UUID;

public class DoneTimeout extends TxnMessage implements ControlMessage {
    public DoneTimeout(UUID uuid) {
        super(uuid);
    }
}
