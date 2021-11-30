package it.unitn.arpino.ds1project.messages.coordinator;

import akka.dispatch.ControlMessage;
import it.unitn.arpino.ds1project.messages.TxnMessage;

import java.util.UUID;

public class TxnEndTimeout extends TxnMessage implements ControlMessage {
    public TxnEndTimeout(UUID uuid) {
        super(uuid);
    }
}
