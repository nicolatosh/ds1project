package it.unitn.arpino.ds1project.nodes.context;

import java.util.UUID;

public abstract class RequestContext {
    public enum Status {
        ACTIVE,
        COMPLETED,
        EXPIRED
    }

    public final UUID uuid;

    protected Status status;

    public RequestContext(UUID uuid) {
        this.uuid = uuid;
        status = Status.ACTIVE;
    }

    public Status getStatus() {
        return status;
    }

    void setStatus(Status status) {
        this.status = status;
    }
}
