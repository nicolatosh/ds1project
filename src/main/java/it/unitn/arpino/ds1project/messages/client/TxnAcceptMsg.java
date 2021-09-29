package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.Typed;

import java.io.Serializable;
import java.util.UUID;

/**
 * Reply from the coordinator receiving TxnBeginMsg
 */
public class TxnAcceptMsg implements Typed, Transactional, Serializable {
    private final UUID uuid;

    public TxnAcceptMsg(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public TYPE getType() {
        return TYPE.TxnControl;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}
