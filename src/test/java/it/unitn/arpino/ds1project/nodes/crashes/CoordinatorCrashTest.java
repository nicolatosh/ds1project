package it.unitn.arpino.ds1project.nodes.crashes;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.ServerInfo;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnEndMsg;
import it.unitn.arpino.ds1project.messages.coordinator.WriteMsg;
import it.unitn.arpino.ds1project.messages.server.DecisionRequest;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.coordinator.CoordinatorRequestContext;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext;
import it.unitn.arpino.ds1project.simulation.Simulation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
        List.of(new ServerInfo(server0, 0, 9)
        ).forEach(server -> coordinator.tell(server, TestActorRef.noSender()));
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        TestKit.shutdownActorSystem(system, scala.concurrent.duration.Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        server0 = null;
        coordinator = null;

        // This has to be done in order to properly let Akka to turn off actors
        Thread.sleep(2000);
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
                ActorRef client = testActor();

                TestKit testKit2 = new TestKit(system);
                ActorRef server1 = testKit2.testActor();

                // Update server0's knowledge of the server1
                ServerInfo info1 = new ServerInfo(server1, 10, 19);
                server0.tell(info1, ActorRef.noSender());

                // Update coordinator with server1
                coordinator.tell(info1, ActorRef.noSender());

                UUID uuid;

                // Starting transaction
                coordinator.tell(new TxnBeginMsg(), client);
                TxnAcceptMsg accept = expectMsgClass(TxnAcceptMsg.class);
                uuid = accept.uuid;

                WriteMsg write = new WriteMsg(uuid, 0, 10);
                coordinator.tell(write, client);
                expectNoMessage();

                // Make coordinator crash in this method before request Votes to servers
                Simulation.COORDINATOR_ON_VOTE_REQUEST_CRASH_PROBABILITY = 100;
                TxnEndMsg end = new TxnEndMsg(uuid, true);
                coordinator.tell(end, client);
                expectNoMessage();

                CoordinatorRequestContext ctx = coordinator.underlyingActor().getRequestContext(write).orElseThrow();
                assertSame(ctx.loggedState().get(), CoordinatorRequestContext.LogState.START_2PC);

                // Coodinator recovers after some time
                TimeUnit.SECONDS.sleep(ServerRequestContext.VOTE_REQUEST_TIMEOUT_S + 1);

                // Server meanwhile should have aborted since VoteTimeout has already fired
                // Make sure above Thread.sleep() is long enough
                ServerRequestContext serverCtx = server0.underlyingActor().getRequestContext(write).orElseThrow();
                assertTrue(serverCtx.isDecided());

                // Coordinator should send client ABORT
                coordinator.underlyingActor().resume();

                CoordinatorRequestContext ctx2 = coordinator.underlyingActor().getRequestContext(write).orElseThrow();
                assertSame(ctx2.getProtocolState(), CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

                // Restoring crash initial probability
                Simulation.COORDINATOR_ON_VOTE_REQUEST_CRASH_PROBABILITY = 0.0;
            }
        };
    }

    @Test
    void testDecided() throws InterruptedException {
        new TestKit(system) {
            {
                ActorRef client = testActor();

                TestKit testKit2 = new TestKit(system);
                ActorRef server1 = testKit2.testActor();

                // Update server0's knowledge of the server1
                ServerInfo info1 = new ServerInfo(server1, 10, 19);
                server0.tell(info1, ActorRef.noSender());

                // Update coordinator with server1
                coordinator.tell(info1, ActorRef.noSender());

                UUID uuid;

                // Starting transaction
                coordinator.tell(new TxnBeginMsg(), client);
                TxnAcceptMsg accept = expectMsgClass(TxnAcceptMsg.class);
                uuid = accept.uuid;

                WriteMsg write = new WriteMsg(uuid, 0, 10);
                coordinator.tell(write, client);
                expectNoMessage();

                // Make coordinator crash in this method after GLOBAL_COMMIT has been written
                // into local log
                Simulation.COORDINATOR_ON_VOTE_RESPONSE_CRASH_PROBABILITY = 100;
                TxnEndMsg end = new TxnEndMsg(uuid, true);
                coordinator.tell(end, client);
                expectNoMessage();

                CoordinatorRequestContext ctx = coordinator.underlyingActor().getRequestContext(write).orElseThrow();
                assertSame(ctx.loggedState().get(), CoordinatorRequestContext.LogState.GLOBAL_COMMIT);

                // Coodinator recovers after some time
                TimeUnit.SECONDS.sleep(ServerRequestContext.FINAL_DECISION_TIMEOUT_S + 1);

                // Server0 meanwhile should have started TERMINATION PROTOCOL
                // Server1 should receive DECISION_REQUEST
                // Make sure above Thread.sleep() is long enough
                ServerRequestContext serverCtx = server0.underlyingActor().getRequestContext(write).orElseThrow();
                assertFalse(serverCtx.isDecided());
                testKit2.expectMsgClass(DecisionRequest.class);

                CoordinatorRequestContext ctx2 = coordinator.underlyingActor().getRequestContext(write).orElseThrow();
                assertSame(ctx2.getProtocolState(), CoordinatorRequestContext.TwoPhaseCommitFSM.WAIT);


                // Termination protocol should not be effective since no server knows the outcome
                // Server0 should be in READY
                ServerRequestContext serverCtx2 = server0.underlyingActor().getRequestContext(write).orElseThrow();
                assertEquals(serverCtx2.loggedState().get(), ServerRequestContext.LogState.VOTE_COMMIT);
                assertEquals(serverCtx2.getProtocolState(), ServerRequestContext.TwoPhaseCommitFSM.READY);

                // Coordinator should send all servers GLOBAL_COMMIT
                coordinator.underlyingActor().resume();

                // Server0 gets the final outcome
                ServerRequestContext serverCtx3 = server0.underlyingActor().getRequestContext(write).orElseThrow();
                assertSame(serverCtx3.getProtocolState(), ServerRequestContext.TwoPhaseCommitFSM.COMMIT);

                // Restoring crash initial probability
                Simulation.COORDINATOR_ON_VOTE_RESPONSE_CRASH_PROBABILITY = 0.0;
            }
        };
    }
}
