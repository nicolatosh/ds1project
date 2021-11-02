package it.unitn.arpino.ds1project.nodes.server;

import it.unitn.arpino.ds1project.datastore.connection.IConnection;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;

import java.util.UUID;

/**
 * Holds the context of a request.
 */
public class ServerRequestContext extends RequestContext {
    public enum TwoPhaseCommitFSM {
        INIT,
        READY,
        ABORT,
        COMMIT
    }

    /**
     * Duration (in seconds) within which the Coordinator's FinalDecision should be received.
     */
    public static final int TIMEOUT_DURATION_DECISION_S = 1;


    /**
     * The current state of the Two-phase commit protocol.
     */
    private TwoPhaseCommitFSM protocolState;

    private final IConnection connection;

    public ServerRequestContext(UUID uuid, IConnection connection) {
        super(uuid);
        this.protocolState = TwoPhaseCommitFSM.INIT;
        this.connection = connection;
    }

    @Override
    public boolean isDecided() {
        return protocolState == TwoPhaseCommitFSM.COMMIT || protocolState == TwoPhaseCommitFSM.ABORT;
    }

    /**
     * @return The current state of the Two-phase commit (2PC) protocol.
     */
    public TwoPhaseCommitFSM getProtocolState() {
        return protocolState;
    }

    public void setProtocolState(TwoPhaseCommitFSM protocolState) {
        this.protocolState = protocolState;
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
        switch (connection.prepare()) {
            case PREPARED:
                protocolState = TwoPhaseCommitFSM.READY;
                break;
            case ABORT:
                protocolState = TwoPhaseCommitFSM.ABORT;
                break;
        }
    }

    public void commit() {
        connection.commit();
        protocolState = TwoPhaseCommitFSM.COMMIT;
    }

    public void abort() {
        if (protocolState == TwoPhaseCommitFSM.READY) {
            connection.abort();
            protocolState = TwoPhaseCommitFSM.ABORT;
        }
    }

    public void startTimer(Server server) {
        super.startTimer(server, TIMEOUT_DURATION_DECISION_S);
    }

    @Override
    public String toString() {
        return "uuid: " + uuid +
                "\ntwo-phase commit protocol state: " + protocolState +
                "\ntransaction:\n" + connection;
    }
}
