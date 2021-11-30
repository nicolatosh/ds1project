package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.TxnMessage;

import java.util.UUID;

public class Done extends TxnMessage {
    public Done(UUID uuid) {
        super(uuid);
    }

    @Override
    public String toString() {
        return "Done{" +
                "uuid=" + uuid +
                '}';
    }
}
