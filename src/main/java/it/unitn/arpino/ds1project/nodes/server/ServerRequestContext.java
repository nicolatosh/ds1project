package it.unitn.arpino.ds1project.nodes.server;

import it.unitn.arpino.ds1project.datastore.connection.IConnection;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.util.UUID;

public class ServerRequestContext extends RequestContext {
    /**
     * State of the Two-phase commit (2PC) protocol of a transaction.
     */
    public enum TwoPhaseCommitFSM {
        INIT,
        READY,
        ABORT,
        COMMIT
    }

    /**
     * Duration (in seconds) within which the {@link Coordinator}'s {@link FinalDecision} should be received.
     */
    public static final int TIMEOUT_DURATION_DECISION_S = 1;

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
     * @return The current state of the Two-phase commit (2PC) protocol of the transaction.
     */
    public TwoPhaseCommitFSM getProtocolState() {
        return protocolState;
    }

    /**
     * Reads from the database the data item with the specified key.
     *
     * @param key The key of the data item to read.
     * @return The read value.
     */
    public int read(int key) {
        return connection.read(key);
    }

    /**
     * Writes a value to the data item with the specified key.
     *
     * @param key   The key of the data item to write.
     * @param value The value to write into the data item.
     */
    public void write(int key, int value) {
        connection.write(key, value);
    }

    /**
     * Attempts to prepare the transaction to be committed, and updates the state of the Two-phase commit (2PC) protocol
     * accordingly.
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

    /**
     * Commits the transaction, and updates the state of the Two-phase commit (2PC) protocol accordingly.
     */
    public void commit() {
        connection.commit();
        protocolState = TwoPhaseCommitFSM.COMMIT;
    }

    /**
     * Aborts the transaction, and updates the state of the Two-phase commit (2PC) protocol accordingly.
     */
    public void abort() {
        // case 1: The Server has attempted to prepare and the outcome was negative. Thus, the state is ABORT.
        // case 2: The Server has attempted to prepare and the outcome was positive. Thus, the state is READY.
        // case 3: The client asked to abort (a requested to prepare was never generated). Thus, the state is INIT.
        if (protocolState == TwoPhaseCommitFSM.READY || protocolState == TwoPhaseCommitFSM.INIT) {
            connection.abort();
            protocolState = TwoPhaseCommitFSM.ABORT;
        }
    }

    /**
     * Starts a countdown timer, within which the {@link Server} should receive the {@link FinalDecision} from the
     * {@link Coordinator}. If the decision does not arrive in time, the Server assumes the Coordinator to be crashed.
     */
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
