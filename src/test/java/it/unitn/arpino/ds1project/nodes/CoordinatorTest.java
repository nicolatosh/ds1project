package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.messages.ServerInfo;
import it.unitn.arpino.ds1project.messages.client.ReadResultMsg;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnEndMsg;
import it.unitn.arpino.ds1project.messages.coordinator.WriteMsg;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class CoordinatorTest {
    ActorSystem system;
    TestActorRef<Server> server0, server1;
    TestActorRef<Server> coordinator;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        server0 = TestActorRef.create(system, Server.props(0, 9), "server0");
        server1 = TestActorRef.create(system, Server.props(10, 19), "server1");
        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");
        List.of(new ServerInfo(server0, 0, 9),
                new ServerInfo(server1, 10, 19)
        ).forEach(server -> coordinator.tell(server, TestActorRef.noSender()));
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, scala.concurrent.duration.Duration.create(1, TimeUnit.SECONDS), false);
        system = null;
        server0 = null;
        server1 = null;
        coordinator = null;
    }

    @Test
    void testConcurrentTxn() {
        new TestKit(system) {
            {
                // Mock the clients

                ActorRef client1 = testActor();

                var testKit2 = new TestKit(system);
                ActorRef client2 = testKit2.testActor();

                // The clients begin two transactions. They must obtain different UUIDs.

                coordinator.tell(new TxnBeginMsg(), client1);
                TxnAcceptMsg accept1 = expectMsgClass(TxnAcceptMsg.class);
                UUID uuid1 = accept1.uuid();

                coordinator.tell(new TxnBeginMsg(), client2);
                TxnAcceptMsg accept2 = testKit2.expectMsgClass(TxnAcceptMsg.class);
                UUID uuid2 = accept2.uuid();

                assertNotEquals(uuid1, uuid2);

                // The first transaction writes two elements, stored in two different servers,
                // and subsequently reads them.

                coordinator.tell(new WriteMsg(uuid1, 4, 40), client1);
                expectNoMessage();

                coordinator.tell(new WriteMsg(uuid1, 17, 170), client1);
                expectNoMessage();

                coordinator.tell(new ReadMsg(uuid1, 4), client1);
                ReadResultMsg read1_1 = expectMsgClass(ReadResultMsg.class);
                assertEquals(40, read1_1.value);

                coordinator.tell(new ReadMsg(uuid1, 17), client1);
                ReadResultMsg read1_2 = expectMsgClass(ReadResultMsg.class);
                assertEquals(170, read1_2.value);

                // The second transaction reads two elements, one of which was written by the first transaction.
                // Since the first transaction has not yet committed, both the read values must be different from
                // the values read by the first transaction.

                coordinator.tell(new ReadMsg(uuid2, 4), client2);
                ReadResultMsg read2_1 = testKit2.expectMsgClass(ReadResultMsg.class);
                assertNotEquals(40, read2_1.value);
                assertEquals(DatabaseBuilder.DEFAULT_DATA_VALUE, read2_1.value);

                coordinator.tell(new ReadMsg(uuid2, 13), client2);
                ReadResultMsg read2_2 = testKit2.expectMsgClass(ReadResultMsg.class);
                assertEquals(DatabaseBuilder.DEFAULT_DATA_VALUE, read2_2.value);

                // The first transaction commits. The response from the server must be positive.

                coordinator.tell(new TxnEndMsg(uuid1, true), client1);
                TxnResultMsg result1 = expectMsgClass(TxnResultMsg.class);
                assertTrue(result1.commit);

                // The second transaction commits. The response from the server must be negative.

                coordinator.tell(new TxnEndMsg(uuid2, true), client2);
                TxnResultMsg result2 = testKit2.expectMsgClass(TxnResultMsg.class);
                assertFalse(result2.commit);
            }
        };
    }
}
