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
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.coordinator.CoordinatorRequestContext;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void tearDown() {
        TestKit.shutdownActorSystem(system, scala.concurrent.duration.Duration.create(1, TimeUnit.SECONDS), false);
        system = null;
        server0 = null;
        coordinator = null;
    }

    /**
     * This test makes sure that in case Coordinator crashes and no server receives
     * VOTE_REQUEST then Servers ABORTS. When coordinator resumes its execution, client
     * is sent an ABORT too.
     *
     * @throws InterruptedException
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
                TxnEndMsg end = new TxnEndMsg(uuid, true);
                coordinator.tell(end, client);
                expectNoMessage();

                CoordinatorRequestContext ctx = coordinator.underlyingActor().getRequestContext(write).orElseThrow();
                assertSame(ctx.loggedState().get(), CoordinatorRequestContext.LogState.START_2PC);

                // Coodinator recovers after some time
                Thread.sleep((ServerRequestContext.VOTE_REQUEST_TIMEOUT_S + 1) * 1000);

                // Server meanwhile should have aborted since VoteTimeout has already fired
                // Make sure above Thread.sleep() is long enough
                ServerRequestContext serverCtx = server0.underlyingActor().getRequestContext(write).orElseThrow();
                assertTrue(serverCtx.isDecided());

                // Coordinator should send client ABORT
                coordinator.underlyingActor().resume();

                CoordinatorRequestContext ctx2 = coordinator.underlyingActor().getRequestContext(write).orElseThrow();
                assertSame(ctx2.getProtocolState(), CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

            }
        };
    }
}
