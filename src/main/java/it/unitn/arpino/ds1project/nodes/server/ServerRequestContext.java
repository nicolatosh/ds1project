package it.unitn.arpino.ds1project.nodes.server;

import it.unitn.arpino.ds1project.datastore.IConnection;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;

import java.util.UUID;

/**
 * Holds the context of a request.
 */
public class ServerRequestContext extends RequestContext {
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

    private final IConnection connection;

    public ServerRequestContext(UUID uuid, IConnection connection) {
        super(uuid);
        this.state = STATE.INIT;
        this.connection = connection;
    }

    /**
     * @return The current state of the Two-phase commit (2PC) protocol.
     */
    public STATE getState() {
        return state;
    }

    public int read(int key) {
        return connection.read(key);
    }

    public void write(int key, int value) {
        connection.write(key, value);
    }

    /**
     * Decides whether server wants to commit or abort
     * based on locking and serializability
     */
    public void prepare() {
        if (connection.prepare()) {
            state = STATE.READY;
        } else {
            state = STATE.GLOBAL_ABORT;
        }
    }

    public void commit() {
        connection.commit();
        state = STATE.COMMIT;
    }

    public void abort() {
        connection.abort();
        state = STATE.GLOBAL_ABORT;

    }

    @Override
    public String toString() {
        return "uuid: " + uuid +
                "\nstate: " + state +
                "\ntransaction:\n" + connection;
    }
}
