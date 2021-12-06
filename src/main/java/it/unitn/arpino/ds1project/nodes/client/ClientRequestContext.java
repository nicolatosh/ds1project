package it.unitn.arpino.ds1project.nodes.client;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ClientRequestContext extends RequestContext {
    public static class Op {
        public final int firstKey, secondKey;
        public Integer firstValue, secondValue;

        public Op(int firstKey, int secondKey) {
            this.firstKey = firstKey;
            this.secondKey = secondKey;
        }

        public boolean isDone() {
            return firstValue != null && secondValue != null;
        }
    }

    public enum Status {
        CREATED,
        REQUESTED,
        CONVERSATIONAL,
        PENDING, // client requested to commit
        COMMIT,
        ABORT // either the request to commit but the request was denied, or it requested to abort, or it timed out
    }

    public final static int TXN_ACCEPT_TIMEOUT_S = 3;
    public final static int READ_TIMEOUT_S = 4;
    public final static int TXN_RESULT_TIMEOUT_S = 8;

    private Status status;

    private final int numOp;
    private final List<Op> operations;

    public ClientRequestContext(ActorRef coordinator, int numOp) {
        super(UUID.randomUUID(), coordinator);
        status = Status.CREATED;

        this.numOp = numOp;
        operations = new ArrayList<>();
    }

    @Override
    public boolean isDecided() {
        return status == Status.COMMIT || status == Status.ABORT;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public int opLeft() {
        return numOp - operations.size();
    }

    public Op newOp(int firstKey, int secondKey) {
        var op = new Op(firstKey, secondKey);
        operations.add(op);
        return op;
    }

    public Op getOp() {
        return operations.get(operations.size() - 1);
    }
}
