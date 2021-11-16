package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.nodes.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class CoordinatorTest {
    ActorSystem system;
    TestActorRef<Coordinator> coordinator;
    TestActorRef<Server> server;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();

        server = TestActorRef.create(system, Server.props(0, 9), "server");
        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");

        IntStream.rangeClosed(0, 9).forEach(key -> coordinator.underlyingActor().getDispatcher().map(key, server));
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        coordinator = null;
        server = null;
    }

    @Test
    @Order(1)
    void testNoDuplicateContexts() {
        new TestKit(system) {
            {
                // Note: right now, coordinators can accept two simultaneous transactions from the same client.

                coordinator.tell(new TxnBeginMsg(), testActor());
                UUID uuid1 = expectMsgClass(TxnAcceptMsg.class).uuid;

                coordinator.tell(new TxnBeginMsg(), testActor());
                UUID uuid2 = expectMsgClass(TxnAcceptMsg.class).uuid;

                assertNotEquals(uuid1, uuid2);

                List<CoordinatorRequestContext> contexts = coordinator.underlyingActor().getActive();
                assertEquals(2, contexts.size());
                assertNotEquals(contexts.get(0), contexts.get(1));
            }
        };
    }

    @Test
    @Order(2)
    void testNoDuplicateParticipant() {
        new TestKit(system) {
            {
                CoordinatorRequestContext ctx = new CoordinatorRequestContext(UUID.randomUUID(), testActor());
                coordinator.underlyingActor().addContext(ctx);
                ctx.log(CoordinatorRequestContext.LogState.NONE);
                ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.INIT);

                coordinator.tell(new ReadMsg(ctx.uuid, 0), testActor());
                coordinator.tell(new ReadMsg(ctx.uuid, 0), testActor());
                assertEquals(1, ctx.getParticipants().size());
            }
        };
    }

    @Test
    @Order(3)
    void testVoteResponseTimeout() {
        new TestKit(system) {
            {
                CoordinatorRequestContext ctx = new CoordinatorRequestContext(UUID.randomUUID(), testActor());
                coordinator.underlyingActor().addContext(ctx);
                ctx.log(CoordinatorRequestContext.LogState.START_2PC);
                ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.WAIT);
                ctx.addParticipant(testActor());

                ctx.startVoteResponseTimer(coordinator.underlyingActor());
                // must account for the timeout duration to elapse and the message to be delivered:
                // add one second more to the duration
                expectMsg(Duration.create(CoordinatorRequestContext.VOTE_RESPONSE_TIMEOUT_S + 1, TimeUnit.SECONDS),
                        new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT));
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());
            }
        };
    }
}
