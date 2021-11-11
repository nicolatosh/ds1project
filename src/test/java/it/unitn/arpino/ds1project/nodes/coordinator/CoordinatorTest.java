package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class CoordinatorTest {
    ActorSystem system;
    TestActorRef<Coordinator> coordinator;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();

        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, scala.concurrent.duration.Duration.create(1, TimeUnit.SECONDS), false);
        system = null;
        coordinator = null;
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
        CoordinatorRequestContext ctx = new CoordinatorRequestContext(UUID.randomUUID(), ActorRef.noSender());
        coordinator.underlyingActor().addContext(ctx);

        coordinator.tell(new ReadMsg(ctx.uuid, 0), ActorRef.noSender());
        coordinator.tell(new ReadMsg(ctx.uuid, 0), ActorRef.noSender());
        assertEquals(1, ctx.getParticipants().size());
    }

    @Test
    @Order(3)
    void testVoteResponseTimeout() {
        new TestKit(system) {
            {
                coordinator.underlyingActor().getDispatcher().map(0, testActor());

                CoordinatorRequestContext ctx = new CoordinatorRequestContext(UUID.randomUUID(), testActor());
                coordinator.underlyingActor().addContext(ctx);
                ctx.addParticipant(testActor());

                ctx.startVoteResponseTimeout(coordinator.underlyingActor());
                // must account for the timeout duration to elapse and the message to be delivered:
                // add one second more to the duration
                expectMsg(Duration.create(CoordinatorRequestContext.VOTE_RESPONSE_TIMEOUT_S + 1, TimeUnit.SECONDS),
                        new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT));
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());
            }
        };
    }
}
