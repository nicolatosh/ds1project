package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.datastore.controller.IDatabaseController;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.AbstractNode;
import it.unitn.arpino.ds1project.nodes.context.ContextManager;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;

import java.util.List;
import java.util.Optional;

public class Server extends AbstractNode {
    private final IDatabaseController controller;

    /**
     * The other servers in the Data Store that this server can contact in a Two-Phase Commit (2PC) recovery
     */
    private List<ActorRef> servers;

    ContextManager<ServerRequestContext> contextManager;

    public Server(int lowerKey, int upperKey) {
        controller = DatabaseBuilder.newBuilder()
                .keyRange(lowerKey, upperKey)
                .create();
        contextManager = new ContextManager<>();
    }

    public static Props props(int lowerKey, int upperKey) {
        return Props.create(Server.class, () -> new Server(lowerKey, upperKey));
    }

    public Receive createReceive() {
        return new ReceiveBuilder()
                .match(ServerStartMsg.class, this::onServerStartMsg)
                .match(ReadRequest.class, this::onReadRequest)
                .match(WriteRequest.class, this::onWriteRequest)
                .match(VoteRequest.class, this::onVoteRequest)
                .match(FinalDecision.class, this::onFinalDecision)
                .match(AbortRequest.class, this::onAbortRequest)
                .build();
    }

    private Optional<ServerRequestContext> getRequestContext(Transactional msg) {
        return contextManager.contextOf(msg);
    }

    private ServerRequestContext newContext(Transactional msg) {
        ServerRequestContext ctx = new ServerRequestContext(msg.uuid(), controller.beginTransaction());

        contextManager.setActive(ctx);
        return ctx;
    }

    private void onServerStartMsg(ServerStartMsg msg) {
        servers = msg.servers;
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
    }

    private void onAbortRequest(AbortRequest msg) {
        Optional<ServerRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ctx.get().abort();

        contextManager.setCompleted(ctx.get());
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

        contextManager.setCompleted(ctx.get());
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
    }
}
