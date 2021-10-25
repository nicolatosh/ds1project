package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.util.UUID;

/**
 * A message that a client sends to a coordinator to end the transaction (TXN): it may ask for commit or abort
 *
 * @see Coordinator
 * @see TxnClient
 */
public class TxnEndMsg extends Message implements Transactional {
    private final UUID uuid;

    /**
     * If false, the transaction should abort
     */
    public final boolean commit;

    public TxnEndMsg(UUID uuid, boolean commit) {
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
