package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.Transactional;

import java.util.UUID;

/**
 * Message from the coordinator to the client with the outcome of the TXN
 */
public class TxnResultMsg extends Message implements Transactional {
    private final UUID uuid;
    public final Boolean commit;

    /**
     * @param commit If false, the transaction was aborted
     */
    public TxnResultMsg(UUID uuid, boolean commit) {
        this.uuid = uuid;
        this.commit = commit;
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
