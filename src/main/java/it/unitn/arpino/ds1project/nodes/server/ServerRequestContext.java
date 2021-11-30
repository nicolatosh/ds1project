package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.arpino.ds1project.datastore.connection.IConnection;
import it.unitn.arpino.ds1project.datastore.controller.IDatabaseController;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ServerRequestContext extends RequestContext {
    public enum LogState {
        INIT,
        VOTE_COMMIT,
        GLOBAL_ABORT,
        DECISION
    }

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
     * Duration (in seconds) within which the {@link Server} expects to receive the {@link Coordinator}'s {@link VoteRequest},
     * after having received the first {@link ReadRequest} or {@link WriteRequest}.
     */
    public static final int VOTE_REQUEST_TIMEOUT_S = 4;

    /**
     * Duration (in seconds) within which the {@link Server} expects to receive the {@link Coordinator}'s {@link FinalDecision},
     * after having received the {@link VoteRequest}.
     */
    public static final int FINAL_DECISION_TIMEOUT_S = 2;

    private final List<LogState> localLog;

    private TwoPhaseCommitFSM protocolState;

    private IConnection connection;

    private Cancellable voteRequestTimer;
    private Cancellable finalDecisionTimer;

    public ServerRequestContext(UUID uuid, ActorRef coordinator, IConnection connection) {
        super(uuid, coordinator);
        localLog = new ArrayList<>();
        this.connection = connection;
    }

    @Override
    public boolean isDecided() {
        return loggedState() == LogState.DECISION || loggedState() == LogState.GLOBAL_ABORT;
    }

    public void log(LogState state) {
        localLog.add(state);
    }

    public LogState loggedState() {
        return localLog.get(localLog.size() - 1);
    }

    /**
     * @return The current state of the Two-phase commit (2PC) protocol of the transaction.
     */
    public TwoPhaseCommitFSM getProtocolState() {
        return protocolState;
    }

    /**
     * Sets the current state of the Two-phase commit (2PC) protocol of the transaction.
     */
    public void setProtocolState(TwoPhaseCommitFSM protocolState) {
        this.protocolState = protocolState;
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
     * Attempts to prepare the transaction to be committed.
     */
    public boolean prepare() {
        return connection.prepare() == IDatabaseController.Response.PREPARED;
    }

    /**
     * Commits the transaction.
     */
    public void commit() {
        connection.commit();
    }

    /**
     * Aborts the transaction.
     */
    public void abort() {
        // case 1: The client asked to abort (a requested to prepare was never generated). Thus, the state is INIT.
        // case 2: The Server has attempted to prepare and the outcome was positive. Thus, the state is READY.
        // case 3: The Server has attempted to prepare and the outcome was negative. Thus, the state is ABORT.
        if (protocolState == TwoPhaseCommitFSM.INIT || protocolState == TwoPhaseCommitFSM.READY) {
            connection.abort();
            connection = null;
        }
    }

    /**
     * Starts a countdown timer, within which the {@link Server} should receive the {@link Coordinator}'s
     * {@link VoteRequest}. If the vote does not arrive in time, the Server assumes the Coordinator to be crashed.
     */
    public void startVoteRequestTimer(Server server) {
        voteRequestTimer = server.getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(VOTE_REQUEST_TIMEOUT_S), // delay
                server.getSelf(), // receiver
                new VoteRequestTimeout(uuid), // message
                server.getContext().dispatcher(), // executor
                server.getSelf()); // sender
    }

    /**
     * Cancels the timer.
     */
    public void cancelVoteRequestTimer() {
        voteRequestTimer.cancel();
    }

    /**
     * Starts a countdown timer, within which the {@link Server} should receive the {@link Coordinator}'s
     * {@link FinalDecision}. If the decision does not arrive in time, the Server assumes the Coordinator to be crashed.
     */
    public void startFinalDecisionTimer(Server server) {
        finalDecisionTimer = server.getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(FINAL_DECISION_TIMEOUT_S), // delay
                server.getSelf(), // receiver
                new FinalDecisionTimeout(uuid), // message
                server.getContext().dispatcher(), // executor
                server.getSelf()); // sender
    }

    /**
     * Cancels the timer. If the timer was not started, it does nothing.
     */
    public void cancelFinalDecisionTimer() {
        if (finalDecisionTimer != null) {
            finalDecisionTimer.cancel();
        }
    }

    @Override
    public String toString() {
        return "uuid: " + uuid +
                "\nlogged state: " + loggedState() +
                "\nprotocol state: " + protocolState +
                "\ntransaction:\n" + connection;
    }
}
