package it.unitn.arpino.ds1project.nodes.context;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.coordinator.CoordinatorRequestContext;
import it.unitn.arpino.ds1project.nodes.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class CoordinatorRequestContextTest {
    ActorSystem system;
    TestActorRef<Coordinator> coordinator;
    TestActorRef<Server> server;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, scala.concurrent.duration.Duration.create(1, TimeUnit.SECONDS), false);
        coordinator = null;
    }

    @Test
    void testContextExpiration() {
        new TestKit(system) {
            {
                ActorRef server = testActor();
                coordinator.underlyingActor().getDispatcher().map(server, 0);

                CoordinatorRequestContext ctx = new CoordinatorRequestContext(UUID.randomUUID(), testActor());
                ctx.addParticipant(server);
                coordinator.underlyingActor().getContextManager().setActive(ctx);

                ctx.startTimer(coordinator.underlyingActor());
                // must account for the timeout duration to elapse and the message to be processed.
                FinalDecision decision = expectMsgClass(Duration.create(CoordinatorRequestContext.TIMEOUT_DURATION_S + 1, TimeUnit.SECONDS), FinalDecision.class);
                assertEquals(FinalDecision.Decision.GLOBAL_ABORT, decision.decision);
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());
            }
        };
    }
}