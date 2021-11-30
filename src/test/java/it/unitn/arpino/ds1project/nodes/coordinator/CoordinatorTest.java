package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.nodes.server.Server;
import org.junit.jupiter.api.*;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CoordinatorTest {
    ActorSystem system;
    TestActorRef<Coordinator> coordinator;
    TestActorRef<Server> server;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();

        server = TestActorRef.create(system, Server.props(0, 9), "server");

        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");
        coordinator.tell(new JoinMessage(0, 9), server);

        List.of(server, coordinator).forEach(node -> node.tell(new StartMessage(), ActorRef.noSender()));
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

                List<CoordinatorRequestContext> contexts = new ArrayList<>(coordinator.underlyingActor().getRepository().getAllRequestContexts(Predicate.not(CoordinatorRequestContext::isDecided)));
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
                coordinator.underlyingActor().getRepository().addRequestContext(ctx);
                ctx.log(CoordinatorRequestContext.LogState.CONVERSATIONAL);
                ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.INIT);

                coordinator.tell(new ReadMsg(ctx.uuid, 0), testActor());
                coordinator.tell(new ReadMsg(ctx.uuid, 0), testActor());
                assertEquals(1, ctx.getParticipants().size());
            }
        };
    }
}
