package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.TxnMessage;

import java.util.UUID;

public class Reset extends TxnMessage {
    public Reset(UUID uuid) {
        super(uuid);
    }

    @Override
    public String toString() {
        return "Reset{" +
                "uuid=" + uuid +
                '}';
    }
}
