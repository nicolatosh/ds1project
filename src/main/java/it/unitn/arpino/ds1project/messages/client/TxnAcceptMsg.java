package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.Transactional;

import java.util.UUID;

/**
 * Reply from the coordinator receiving TxnBeginMsg
 */
public class TxnAcceptMsg extends Message implements Transactional {
    private final UUID uuid;

    public TxnAcceptMsg(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public Message.TYPE getType() {
        return Message.TYPE.TxnControl;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}
