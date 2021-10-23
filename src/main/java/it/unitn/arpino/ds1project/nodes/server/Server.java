package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.datastore.DatabaseBuilder;
import it.unitn.arpino.ds1project.datastore.IConnection;
import it.unitn.arpino.ds1project.datastore.IDatabaseController;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.Typed;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.STATUS;
import it.unitn.arpino.ds1project.nodes.context.ContextManager;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.List;
import java.util.Optional;

public class Server extends AbstractActor {
    LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final STATUS status;

    private final IDatabaseController controller;

    /**
     * The other servers in the Data Store that this server can contact in a Two-Phase Commit (2PC) recovery
     */
    private List<ActorRef> servers;
    private final int serverId;

    ContextManager<ServerRequestContext> contextManager;

    public Server(int serverId) {
        status = STATUS.ALIVE;
        this.serverId = serverId;
        controller = DatabaseBuilder.newBuilder()
                .keyRange(10 * serverId, 10 * serverId + 9)
                .create();
        contextManager = new ContextManager<>();
    }

    public static Props props(int serverId) {
        return Props.create(Server.class, () -> new Server(serverId));
    }

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
        if (msg instanceof Typed) {
            Typed typed = (Typed) msg;

            log.info("received " + typed.getType() +
                    "/" + typed.getClass().getSimpleName() +
                    " from " + getSender().path().name());
        }

        super.aroundReceive(receive, msg);
    }

    public Receive createReceive() {
        return new ReceiveBuilder()
                .match(ServerStartMsg.class, this::onServerStartMsg)
                .match(ReadRequest.class, this::onReadRequest)
                .match(WriteRequest.class, this::onWriteRequest)
                .match(VoteRequest.class, this::onVoteRequest)
                .match(FinalDecision.class, this::onDecisionRequest)
                .match(AbortRequest.class, this::onAbortRequest)
                .build();
    }

    private Optional<ServerRequestContext> getRequestContext(Transactional msg) {
        return contextManager.contextOf(msg);
    }

    private ServerRequestContext newContext(Transactional msg) {
        ServerRequestContext ctx = new ServerRequestContext(msg.uuid(), controller.beginTransaction());

        contextManager.save(ctx);
        return ctx;
    }

    private void onServerStartMsg(ServerStartMsg msg) {
        servers = msg.servers;

        if (msg instanceof ServerStartWithKeysMsg) {
            ServerStartWithKeysMsg msgWithKeys = (ServerStartWithKeysMsg) msg;

            IConnection connection = controller.beginTransaction();
            msgWithKeys.keys.forEach(key -> connection.write(key, msgWithKeys.values.get(key)));
            connection.commit();
        }
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

        VoteResponse vote = null;

        switch (ctx.get().getState()) {
            case VOTE_COMMIT:
            case READY:
                vote = new VoteResponse(req.uuid(), Vote.YES);
                break;
            case GLOBAL_ABORT:
                vote = new VoteResponse(req.uuid(), Vote.NO);
                ctx.get().abort();
                break;
        }

        getSender().tell(vote, getSelf());
    }

    private void onAbortRequest(AbortRequest msg) {
        Optional<ServerRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        // TODO if we do that "locally" then we need to manage the situation
        // in which coordinator tells us to abort again so the line below
        // will get executed twice (see OnDecisionRequest)
        //ctx.get().abort();

        VoteResponse vote = new VoteResponse(msg.uuid(), Vote.NO);
        getSender().tell(vote, getSelf());

    }

    private void onDecisionRequest(FinalDecision req) {
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
    }
}
