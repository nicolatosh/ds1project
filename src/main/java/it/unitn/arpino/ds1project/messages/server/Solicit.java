package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.TxnMessage;

import java.util.UUID;

public class Solicit extends TxnMessage {
    public Solicit(UUID uuid) {
        super(uuid);
    }
}
