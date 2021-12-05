package it.unitn.arpino.ds1project.messages;

import java.util.UUID;


public class TimeoutMsg extends TxnMessage {
    public TimeoutMsg(UUID uuid) {
        super(uuid);
    }

    @Override
    public String toString() {
        return "TimeoutMsg{" +
                "uuid=" + uuid +
                '}';
    }
}
