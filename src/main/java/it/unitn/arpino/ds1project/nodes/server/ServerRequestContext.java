package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.datastore.connection.IConnection;
import it.unitn.arpino.ds1project.datastore.controller.IDatabaseController;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.messages.server.WriteRequest;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.util.*;

public class ServerRequestContext extends RequestContext {
    public enum LogState {
        CONVERSATIONAL,
        VOTE_COMMIT,
        GLOBAL_COMMIT,
        GLOBAL_ABORT
    }

    /**
     * Duration (in seconds) within which the {@link Server} should receive a {@link Coordinator}'s {@link ReadRequest}
     * or {@link WriteRequest} after the last one it previously received.
     */
    public static final int CONVERSATIONAL_TIMEOUT = 6;

    /**
     * Duration (in seconds) within which the {@link Server} expects to receive the {@link Coordinator}'s {@link FinalDecision},
     * after having received the {@link VoteRequest}.
     */
    public static final int FINAL_DECISION_TIMEOUT_S = 6;

    private final Set<ActorRef> participants;

    private final List<LogState> localLog;

    private IConnection connection;

    public ServerRequestContext(UUID uuid, ActorRef coordinator, IConnection connection) {
        super(uuid, coordinator);
        participants = new HashSet<>();
        localLog = new ArrayList<>();
        this.connection = connection;
    }

    @Override
    public boolean isDecided() {
        return loggedState() == LogState.GLOBAL_COMMIT || loggedState() == LogState.GLOBAL_ABORT;
    }

    /**
     * Optimization; called by the server when receiving a vote request.
     */
    public void addParticipant(ActorRef participant) {
        participants.add(participant);
    }

    public Collection<ActorRef> getParticipants() {
        return new HashSet<>(participants);
    }

    public void log(LogState state) {
        localLog.add(state);
    }

    public LogState loggedState() {
        return localLog.get(localLog.size() - 1);
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
        if (loggedState() == LogState.GLOBAL_COMMIT) {
            connection.commit();
            connection = null;
        }
    }

    /**
     * Aborts the transaction.
     */
    public void abort() {
        if (loggedState() == LogState.GLOBAL_ABORT) {
            connection.abort();
            connection = null;
        }
    }


    @Override
    public String toString() {
        return "uuid: " + uuid +
                "\nlogged state: " + loggedState() +
                "\ntransaction:\n" + connection;
    }
}
