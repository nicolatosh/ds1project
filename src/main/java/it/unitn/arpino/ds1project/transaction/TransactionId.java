package it.unitn.arpino.ds1project.transaction;

import java.time.Instant;

public class TransactionId {
    final int clientId;
    final int coordinatorId;
    final Instant timestamp;

    public TransactionId(int clientId, int coordinatorId) {
        this.clientId = clientId;
        this.coordinatorId = coordinatorId;
        this.timestamp = Instant.now();
    }
}