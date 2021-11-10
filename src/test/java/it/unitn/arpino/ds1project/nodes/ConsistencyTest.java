package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.ServerInfo;
import it.unitn.arpino.ds1project.messages.client.ReadResultMsg;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.coordinator.WriteMsg;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsistencyTest {
    ActorSystem system;
    TestActorRef<Server> server0, server1;
    TestActorRef<Coordinator> coordinator;

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
        coordinator = null;
        server0 = server1 = null;
    }

    @Test
    void testSequentialTxns() {
        new TestKit(system) {
            {
                // Mock the client
                ActorRef client1 = testActor();

                coordinator.tell(new TxnBeginMsg(), client1);
                TxnAcceptMsg accept = expectMsgClass(TxnAcceptMsg.class);

                UUID uuid = accept.uuid;
                Random random = new Random();

                for (int i = 0; i < 10; i++) {
                    int key = random.nextInt(9);
                    int key2 = random.nextInt(9) + 10;
                    ReadMsg readMsg = new ReadMsg(uuid, key);
                    coordinator.tell(readMsg, client1);
                    ReadResultMsg readResultMsg = expectMsgClass(ReadResultMsg.class);

                    WriteMsg writeMsg = new WriteMsg(uuid, key2, readResultMsg.value);
                    coordinator.tell(writeMsg, client1);
                    expectNoMessage();
                }

                ServerRequestContext ctx = server0.underlyingActor().getRequestContext(accept).orElseThrow();
                ServerRequestContext ctx2 = server1.underlyingActor().getRequestContext(accept).orElseThrow();
                int total = IntStream.rangeClosed(0, 9).boxed().map(ctx::read).reduce(0, Integer::sum);
                int total2 = IntStream.rangeClosed(10, 19).boxed().map(ctx2::read).reduce(0, Integer::sum);
                assertEquals(total + total2, 2000);
            }
        };
    }
}
