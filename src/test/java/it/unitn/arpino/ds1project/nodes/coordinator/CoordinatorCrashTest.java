package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnEndMsg;
import it.unitn.arpino.ds1project.messages.coordinator.WriteMsg;
import it.unitn.arpino.ds1project.messages.server.DecisionRequest;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class CoordinatorCrashTest {

    ActorSystem system;
    TestActorRef<Server> server0;
    TestActorRef<Coordinator> coordinator;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();

        server0 = TestActorRef.create(system, Server.props(0, 9), "server0");

        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");
        IntStream.rangeClosed(0, 9).forEach(key -> coordinator.underlyingActor().getDispatcher().map(key, server0));
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        server0 = null;
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
                // Simulate a transaction
                coordinator.tell(new TxnBeginMsg(), testActor());
                UUID uuid = expectMsgClass(TxnAcceptMsg.class).uuid;

                coordinator.tell(new WriteMsg(uuid, 0, 10), testActor());
                expectNoMessage();

                // Set this parameter before requesting the coordinator to end the transaction
                coordinator.underlyingActor().getParameters().coordinatorOnVoteRequestCrashProbability = 1.;

                coordinator.tell(new TxnEndMsg(uuid, true), testActor());

                assertSame(CoordinatorRequestContext.LogState.START_2PC,
                        coordinator.underlyingActor().getRequestContext(uuid).orElseThrow().loggedState().orElseThrow());

                // Let time elapse to allow the coordinator to recover. The amount of time is such that the participants
                // of the transactions abort, as they do not receive the vote request from the coordinator in time.
                TimeUnit.SECONDS.sleep(ServerRequestContext.VOTE_REQUEST_TIMEOUT_S + 1);

                assertTrue(server0.underlyingActor().getRequestContext(uuid).orElseThrow().isDecided());

                coordinator.underlyingActor().resume();

                TimeUnit.SECONDS.sleep(1);
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT,
                        coordinator.underlyingActor().getRequestContext(uuid).orElseThrow().getProtocolState());

                expectMsg(new TxnEndMsg(uuid, false));
            }
        };
    }

    @Test
    void testDecided() throws InterruptedException {
        new TestKit(system) {
            {
                TestKit testKit2 = new TestKit(system);
                ActorRef server1 = testKit2.testActor();

                // Update server0's knowledge of server1
                server0.underlyingActor().addServer(server1);

                // Update coordinator's knowledge of server0 and server1
                IntStream.rangeClosed(0, 9).forEach(key -> coordinator.underlyingActor().getDispatcher().map(key, server0));
                IntStream.rangeClosed(10, 19).forEach(key -> coordinator.underlyingActor().getDispatcher().map(key, server1));

                // Simulate a transaction, where both servers are involved
                coordinator.tell(new TxnBeginMsg(), testActor());
                UUID uuid = expectMsgClass(TxnAcceptMsg.class).uuid;

                coordinator.tell(new WriteMsg(uuid, 0, 10), testActor());
                expectNoMessage();

                // Set this parameter before requesting the coordinator to end the transaction
                coordinator.underlyingActor().getParameters().coordinatorOnVoteResponseCrashProbability = 1.;
                coordinator.tell(new TxnEndMsg(uuid, true), testActor());
                expectNoMessage();

                CoordinatorRequestContext ctx = coordinator.underlyingActor().getRequestContext(uuid).orElseThrow();
                assertSame(CoordinatorRequestContext.LogState.GLOBAL_COMMIT, ctx.loggedState().get());

                // Let time elapse to allow the coordinator to recover. The amount of time is such that the participants
                // of the transactions conclude the termination protocol reaching the decision to abort (as no one knows
                // the final decision).

                TimeUnit.SECONDS.sleep(ServerRequestContext.FINAL_DECISION_TIMEOUT_S + 1);

                assertFalse(server0.underlyingActor().getRequestContext(uuid).orElseThrow().isDecided());
                testKit2.expectMsg(new DecisionRequest(uuid));

                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.WAIT,
                        coordinator.underlyingActor().getRequestContext(uuid).orElseThrow().getProtocolState());

                assertSame(ServerRequestContext.LogState.VOTE_COMMIT,
                        server0.underlyingActor().getRequestContext(uuid).orElseThrow().loggedState().orElseThrow());
                assertSame(ServerRequestContext.TwoPhaseCommitFSM.READY,
                        server0.underlyingActor().getRequestContext(uuid).orElseThrow().getProtocolState());

                coordinator.underlyingActor().resume();

                assertSame(ServerRequestContext.TwoPhaseCommitFSM.COMMIT,
                        server0.underlyingActor().getRequestContext(uuid).orElseThrow().getProtocolState());
            }
        };
    }
}
