package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.datastore.controller.IDatabaseController;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.messages.ServerInfo;
import it.unitn.arpino.ds1project.messages.TimeoutExpired;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;
import it.unitn.arpino.ds1project.nodes.context.ContextManager;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Server extends DataStoreNode {
    private final IDatabaseController controller;

    /**
     * The other servers in the Data Store that this server can contact in a Two-Phase Commit (2PC) recovery
     */
    private final List<ServerInfo> servers;

    ContextManager<ServerRequestContext> contextManager;

    public Server(int lowerKey, int upperKey) {
        controller = DatabaseBuilder.newBuilder()
                .keyRange(lowerKey, upperKey)
                .create();
        contextManager = new ContextManager<>();
        servers = new ArrayList<>();
    }

    public static Props props(int lowerKey, int upperKey) {
        return Props.create(Server.class, () -> new Server(lowerKey, upperKey));
    }

    public Receive createReceive() {
        return new ReceiveBuilder()
                .match(ServerInfo.class, this::onServerInfo)
                .match(ReadRequest.class, this::onReadRequest)
                .match(WriteRequest.class, this::onWriteRequest)
                .match(VoteRequest.class, this::onVoteRequest)
                .match(FinalDecision.class, this::onFinalDecision)
                .match(AbortRequest.class, this::onAbortRequest)
                .match(TimeoutExpired.class, this::onTimeoutExpired)
                .match(DecisionRequest.class, this::onDecisionRequest)
                .match(DecisionResponse.class, this::onDecisionResponse)
                .build();
    }

    private Optional<ServerRequestContext> getRequestContext(Transactional msg) {
        return contextManager.contextOf(msg);
    }

    private ServerRequestContext newContext(Transactional msg) {
        ServerRequestContext ctx = new ServerRequestContext(msg.uuid(), controller.beginTransaction());

        contextManager.add(ctx);
        return ctx;
    }

    private void onServerInfo(ServerInfo server) {
        servers.add(server);
    }

    private void onReadRequest(ReadRequest req) {
        ServerRequestContext ctx = getRequestContext(req).orElse(this.newContext(req));

        int value = ctx.read(req.key);

        ReadResult res = new ReadResult(req.uuid(), req.key, value);
        getSender().tell(res, getSelf());
    }

    private void onWriteRequest(WriteRequest req) {
        ServerRequestContext ctx = getRequestContext(req).orElse(this.newContext(req));

        ctx.write(req.key, req.value);
    }

    private void onVoteRequest(VoteRequest req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ctx.get().prepare();

        switch (ctx.get().getProtocolState()) {
            case READY: {
                log.info("VOTE_COMMIT");

                VoteResponse vote = new VoteResponse(req.uuid(), Vote.YES);
                getSender().tell(vote, getSelf());
                break;
            }
            case ABORT: {
                log.info("GLOBAL_ABORT");

                VoteResponse vote = new VoteResponse(req.uuid(), Vote.NO);
                getSender().tell(vote, getSelf());
                break;
            }
        }

        ctx.get().startTimer(this);
    }

    private void onAbortRequest(AbortRequest msg) {
        Optional<ServerRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ctx.get().abort();

        ctx.get().setCompleted();
    }

    /**
     * Starts the termination protocol: the server asks every other server for the decision
     * and remains blocked until it receives a response.
     */
    private void onTimeoutExpired(TimeoutExpired timeout) {
        Optional<ServerRequestContext> ctx = getRequestContext(timeout);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        log.info("TERMINATION_PROTOCOL_TIMEOUT_FOR_FINALDECISION");
        DecisionRequest req = new DecisionRequest(ctx.get().uuid);

        servers.forEach(s -> s.server.tell(req, getSelf()));
    }

    /**
     * The server replies with transaction's outcome (i.e., the coordinator's final decision) to
     * the server requesting it, which is executing the termination protocol.
     */
    private void onDecisionRequest(DecisionRequest req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ServerRequestContext.TwoPhaseCommitFSM status = ctx.get().getProtocolState();
        if (status == ServerRequestContext.TwoPhaseCommitFSM.COMMIT ||
                status == ServerRequestContext.TwoPhaseCommitFSM.ABORT) {
            DecisionResponse resp = new DecisionResponse(req.uuid(), status);
            getSender().tell(resp, getSelf());
        }
    }

    /**
     * This method is executed if the server is executing the termination protocol.
     * The server receives the transaction's outcome (i.e., the coordinator's final decision) from another server.
     */
    private void onDecisionResponse(DecisionResponse resp) {
        Optional<ServerRequestContext> ctx = getRequestContext(resp);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ServerRequestContext.TwoPhaseCommitFSM status = resp.getStatus();
        switch (status) {
            case ABORT: {
                ctx.get().abort();
                break;
            }
            case COMMIT: {
                ctx.get().commit();
                break;
            }
        }

        ctx.get().setCompleted();
    }


    private void onFinalDecision(FinalDecision req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        switch (req.decision) {
            case GLOBAL_COMMIT:
                ctx.get().commit();
                break;

            case GLOBAL_ABORT:
                ctx.get().abort();
                break;

        }

        ctx.get().cancelTimer();
        ctx.get().setCompleted();
    }

    @Override
    protected void resume() {
        super.resume();
        contextManager.getActive().forEach(RequestContext::setCrashed);
        recoveryAbort();
        recoveryStartTerminationProtocol();
    }

    /**
     * This method implements a recovery action of the Two-phase commit (2PC) protocol.
     * It aborts all the active transactions for which the server has not yet cast the vote.
     */
    private void recoveryAbort() {
    }

    /**
     * This method implements a recovery action of the Two-phase commit (2PC) protocol.
     * It starts the termination protocol for all the active transactions for which the server has already
     * cast the vote.
     */
    private void recoveryStartTerminationProtocol() {

        // Not even voted = INIT status => ABORT

        // Voted = Either GLOBAL_ABORT or GLOBAL_COMMIT. Resend to coordinator such status

        // Ready -> termination protocol

    }
}
