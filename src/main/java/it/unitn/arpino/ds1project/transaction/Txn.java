package it.unitn.arpino.ds1project.transaction;

import java.util.UUID;

public class Txn {
    public final UUID uuid;
    public final int clientId;

    public Txn(int clientId) {
        uuid = UUID.randomUUID();
        this.clientId = clientId;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Txn txn = (Txn) o;
        return uuid.equals(txn.uuid);
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }
}
