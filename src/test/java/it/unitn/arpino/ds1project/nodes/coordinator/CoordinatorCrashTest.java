package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.coordinator.TxnEndMsg;
import it.unitn.arpino.ds1project.messages.coordinator.WriteMsg;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertSame;

public class CoordinatorCrashTest {

    ActorSystem system;
    TestActorRef<Server> server0, server1;
    TestActorRef<Coordinator> coordinator;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();

        server0 = TestActorRef.create(system, Server.props(0, 9), "server0");
        server1 = TestActorRef.create(system, Server.props(10, 19), "server1");
        server0.underlyingActor().addServer(server1);
        server1.underlyingActor().addServer(server0);

        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");
        IntStream.rangeClosed(0, 9).forEach(key -> coordinator.underlyingActor().getDispatcher().map(key, server0));
        IntStream.rangeClosed(10, 19).forEach(key -> coordinator.underlyingActor().getDispatcher().map(key, server1));
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        server0 = server1 = null;
        coordinator = null;
    }

    /**
     * This test makes sure that in case Coordinator crashes and no server receives
     * VOTE_REQUEST then Servers ABORTS. When coordinator resumes its execution, client
     * is sent an ABORT too.
     *
     * @throws InterruptedException because of Thread sleep
     */
    @Test
    void testNotDecided() throws InterruptedException {
        new TestKit(system) {
            {
                // Simulate a transaction, in which only server0 is involved.
                CoordinatorRequestContext ctx = new CoordinatorRequestContext(UUID.randomUUID(), testActor());
                coordinator.underlyingActor().addContext(ctx);
                coordinator.tell(new WriteMsg(ctx.uuid, 0, 10), testActor());

                // We want the coordinator to crash before asking the participants to vote.
                // To do so, we first have to set the following probability to 1.
                coordinator.underlyingActor().getParameters().coordinatorOnVoteRequestCrashProbability = 1.;
                coordinator.tell(new TxnEndMsg(ctx.uuid, true), testActor());

                assertSame(CoordinatorRequestContext.LogState.START_2PC,
                        coordinator.underlyingActor().getRequestContext(ctx.uuid).orElseThrow().loggedState().orElseThrow());

                // Let time elapse to make the participants of the transactions abort, as they do not receive the vote
                // request from the coordinator in time.
                TimeUnit.SECONDS.sleep(ServerRequestContext.VOTE_REQUEST_TIMEOUT_S + 1);

                assertSame(ServerRequestContext.LogState.GLOBAL_ABORT,
                        server0.underlyingActor().getRequestContext(ctx.uuid).orElseThrow().loggedState().orElseThrow());

                coordinator.underlyingActor().resume();

                // after resuming, the coordinator transitions to the abort state...
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT,
                        coordinator.underlyingActor().getRequestContext(ctx.uuid).orElseThrow().getProtocolState());
            }
        };
    }

    @Test
    void testDecided() throws InterruptedException {
        new TestKit(system) {
            {
                // Simulate a transaction, in which both servers are involved.
                CoordinatorRequestContext ctx = new CoordinatorRequestContext(UUID.randomUUID(), testActor());
                coordinator.underlyingActor().addContext(ctx);
                coordinator.tell(new WriteMsg(ctx.uuid, 6, 60), testActor());
                coordinator.tell(new WriteMsg(ctx.uuid, 12, 120), testActor());

                // We want the coordinator to collect all votes from the participants, but to crash before sending the
                // final decision. To do so, we first have to set the following probability to 1.
                coordinator.underlyingActor().getParameters().coordinatorOnFinalDecisionCrashProbability = 1.;
                coordinator.tell(new TxnEndMsg(ctx.uuid, true), testActor());

                // The coordinator has now collected the vote from the participants, which are all positive, and thus
                // has written GLOBAL_COMMIT to the local log.
                assertSame(CoordinatorRequestContext.LogState.GLOBAL_COMMIT, ctx.loggedState().get());
                // However, it crashed before sending the final decision, thus did not transition to the COMMIT state.
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.WAIT,
                        coordinator.underlyingActor().getRequestContext(ctx.uuid).orElseThrow().getProtocolState());

                // The state of the participants must be as follows.
                assertSame(ServerRequestContext.LogState.VOTE_COMMIT,
                        server0.underlyingActor().getRequestContext(ctx.uuid).orElseThrow().loggedState().orElseThrow());
                assertSame(ServerRequestContext.TwoPhaseCommitFSM.READY,
                        server0.underlyingActor().getRequestContext(ctx.uuid).orElseThrow().getProtocolState());

                // We don't resume the coordinator immediately, so that the participants time out and perform the
                // termination protocol. Since that none of them knows the final decision, they remain blocked.
                TimeUnit.SECONDS.sleep(ServerRequestContext.FINAL_DECISION_TIMEOUT_S + 1);

                coordinator.underlyingActor().resume();

                // after resuming, the coordinator transitions to the commit state...
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.COMMIT, ctx.getProtocolState());
                // ...and sends the final decision to the participants, which were blocked.
                assertSame(ServerRequestContext.LogState.DECISION,
                        server0.underlyingActor().getRequestContext(ctx.uuid).orElseThrow().loggedState().orElseThrow());
                assertSame(ServerRequestContext.TwoPhaseCommitFSM.COMMIT,
                        server0.underlyingActor().getRequestContext(ctx.uuid).orElseThrow().getProtocolState());
            }
        };
    }
}
