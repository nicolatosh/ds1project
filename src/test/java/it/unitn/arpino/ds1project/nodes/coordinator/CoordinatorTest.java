package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.messages.client.ReadResultMsg;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.nodes.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class CoordinatorTest {
    ActorSystem system;
    TestActorRef<Coordinator> coordinator;
    ActorRef server;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");
        server = system.actorOf(Server.props(0, 9), "server");

        var join = new JoinMessage(0, 9);
        coordinator.tell(join, server);

        coordinator.tell(new StartMessage(), ActorRef.noSender());
        server.tell(new StartMessage(), ActorRef.noSender());
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        coordinator = null;
    }

    @Test
    void testNoDuplicateContexts() {
        new TestKit(system) {
            {
                // Note: right now, coordinators can accept two simultaneous transactions from the same client.

                var uuid1 = UUID.randomUUID();
                var begin1 = new TxnBeginMsg(uuid1);
                coordinator.tell(begin1, testActor());
                var accept1 = new TxnAcceptMsg(uuid1);
                expectMsg(accept1);

                var uuid2 = UUID.randomUUID();
                var begin2 = new TxnBeginMsg(uuid2);
                coordinator.tell(begin2, testActor());
                var accept2 = new TxnAcceptMsg(uuid2);
                expectMsg(accept2);

                assertNotEquals(uuid1, uuid2);

                var ctx1 = coordinator.underlyingActor().getRepository().getRequestContextById(uuid1);
                var ctx2 = coordinator.underlyingActor().getRepository().getRequestContextById(uuid2);
                assertNotEquals(ctx1, ctx2);
            }
        };
    }

    @Test
    void testNoDuplicateParticipant() {
        new TestKit(system) {
            {
                var uuid = UUID.randomUUID();
                var begin = new TxnBeginMsg(uuid);
                coordinator.tell(begin, testActor());

                var accept = new TxnAcceptMsg(uuid);
                expectMsg(accept);

                var read1 = new ReadMsg(uuid, 0);
                coordinator.tell(read1, testActor());
                var result1 = new ReadResultMsg(uuid, 0, DatabaseBuilder.DEFAULT_DATA_VALUE);
                expectMsg(result1);

                var read2 = new ReadMsg(uuid, 1);
                coordinator.tell(read2, testActor());
                var result2 = new ReadResultMsg(uuid, 1, DatabaseBuilder.DEFAULT_DATA_VALUE);
                expectMsg(result2);

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);
                assertEquals(1, ctx.getParticipants().size());
            }
        };
    }
}
