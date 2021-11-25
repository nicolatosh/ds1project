package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.util.UUID;

/**
 * A message that a {@link Coordinator} uses to accept a transaction opened from a {@link TxnClient}.
 */
public class TxnAcceptMsg extends TxnMessage {
    public TxnAcceptMsg(UUID uuid) {
        super(uuid);
    }
}
