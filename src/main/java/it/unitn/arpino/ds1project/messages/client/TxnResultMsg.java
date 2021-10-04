package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.Typed;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message from the coordinator to the client with the outcome of the TXN
 */
public class TxnResultMsg implements Typed, Transactional, Serializable {
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
    public TYPE getType() {
        return TYPE.TxnControl;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}
