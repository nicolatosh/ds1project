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
import org.junit.jupiter.api.*;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CoordinatorTest {
    ActorSystem system;
    TestActorRef<Coordinator> coordinator;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();

        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");
        coordinator.tell(new JoinMessage(0, 9), ActorRef.noSender());

        coordinator.tell(new StartMessage(), ActorRef.noSender());
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        coordinator = null;
    }

    @Test
    @Order(1)
    void testNoDuplicateContexts() {
        new TestKit(system) {
            {
                // Note: right now, coordinators can accept two simultaneous transactions from the same client.

                var begin1 = new TxnBeginMsg();
                var uuid1 = begin1.uuid;
                coordinator.tell(begin1, testActor());
                expectMsg(new TxnAcceptMsg(uuid1));

                var begin2 = new TxnBeginMsg();
                var uuid2 = begin2.uuid;
                coordinator.tell(begin2, testActor());
                expectMsg(new TxnAcceptMsg(uuid2));

                assertNotEquals(uuid1, uuid2);

                var ctx1 = coordinator.underlyingActor().getRepository().getRequestContextById(uuid1);
                var ctx2 = coordinator.underlyingActor().getRepository().getRequestContextById(uuid2);
                assertNotEquals(ctx1, ctx2);
            }
        };
    }

    @Test
    @Order(2)
    void testNoDuplicateParticipant() {
        new TestKit(system) {
            {
                var begin = new TxnBeginMsg();
                var uuid = begin.uuid;
                coordinator.tell(begin, testActor());

                var read0 = new ReadMsg(uuid, 0);
                coordinator.tell(read0, testActor());

                var read1 = new ReadMsg(uuid, 1);
                coordinator.tell(read1, testActor());

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);
                assertEquals(1, ctx.getParticipants().size());
            }
        };
    }
}
