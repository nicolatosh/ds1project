package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.util.UUID;

/**
 * A message that a {@link TxnClient} uses to end a transaction with a {@link Coordinator}.
 */
public class TxnEndMsg extends TxnMessage {
    public final boolean commit;

    public TxnEndMsg(UUID uuid, boolean commit) {
        super(uuid);
        this.commit = commit;
    }
}
