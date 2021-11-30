package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.TxnMessage;

import java.util.UUID;

public class Reset extends TxnMessage {
    public Reset(UUID uuid) {
        super(uuid);
    }
}
