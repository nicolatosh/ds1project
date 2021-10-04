package it.unitn.arpino.ds1project.nodes.server;

import it.unitn.arpino.ds1project.datastore.Transaction;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;

import java.util.UUID;

/**
 * Holds the context of a request.
 */
public class ServerRequestContext implements RequestContext {
    private final UUID uuid;

    public enum STATE {
        INIT,
        READY,
        GLOBAL_ABORT,
        VOTE_COMMIT,
        COMMIT
    }

    /**
     * The current state of the Two-phase commit protocol.
     */
    private STATE state;

    public final Transaction transaction;

    public ServerRequestContext(UUID uuid, Transaction transaction) {
        this.uuid = uuid;
        this.state = STATE.INIT;
        this.transaction = transaction;
    }

    /**
     * @return The current state of the Two-phase commit (2PC) protocol.
     */
    public STATE getState() {
        return state;
    }

    public int read(int key) {
        return transaction.read(key);
    }

    public void write(int key, int value) {
        transaction.write(key, value);
    }

    public void prepare() {
        if (transaction.prepare()) {
            state = STATE.READY;
        } else {
            state = STATE.GLOBAL_ABORT;
        }
    }

    public void commit() {
        transaction.commit();
        state = STATE.COMMIT;
    }

    public void abort() {
        transaction.abort();
        state = STATE.GLOBAL_ABORT;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}
