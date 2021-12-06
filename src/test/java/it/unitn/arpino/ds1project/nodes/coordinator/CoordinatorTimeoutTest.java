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
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnEndMsg;
import it.unitn.arpino.ds1project.nodes.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;

public class CoordinatorTimeoutTest {
    ActorSystem system;
    TestActorRef<Coordinator> coordinator;
    TestActorRef<Server> server;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");
        server = TestActorRef.create(system, Server.props(0, 9), "server");

        var join = new JoinMessage(0, 9);
        coordinator.tell(join, server);

        var start = new StartMessage();
        server.tell(start, ActorRef.noSender());
        coordinator.tell(start, ActorRef.noSender());
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        coordinator = null;
        server = null;
    }

    @Test
    void shouldTimeoutDueToClient() {
        // The client starts a transaction, performs no read or write,
        // and waits an amount of time that makes the coordinator time out and abort the transaction.
        // The client should receive a result indicating that the transaction was aborted.
        new TestKit(system) {
            {
                // testActor() will be the client of the transaction.

                var begin = new TxnBeginMsg();
                var uuid = begin.uuid;
                coordinator.tell(begin, testActor());

                var accept = new TxnAcceptMsg(uuid);
                expectMsg(accept);

                var result = new TxnResultMsg(uuid, false);
                expectMsg(Duration.create(CoordinatorRequestContext.TXN_END_TIMEOUT_S + 1, TimeUnit.SECONDS),
                        result);

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);

                assertSame(CoordinatorRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());
            }
        };
    }

    @Test
    void shouldTimeoutBeforeCommitting() {
        // The client starts a transaction, performs a read,
        // and waits an amount of time that makes the coordinator time out and abort the transaction.
        // The client should receive a result indicating that the transaction was aborted.

        new TestKit(system) {
            {
                // testActor() will be the client of the transaction.

                var begin = new TxnBeginMsg();
                var uuid = begin.uuid;
                coordinator.tell(begin, testActor());

                var accept = new TxnAcceptMsg(uuid);
                expectMsg(accept);

                var readMsg = new ReadMsg(uuid, 0);
                coordinator.tell(readMsg, testActor());

                var readResultMsg = new ReadResultMsg(uuid, 0, DatabaseBuilder.DEFAULT_DATA_VALUE);
                expectMsg(readResultMsg);

                var result = new TxnResultMsg(uuid, false);
                expectMsg(Duration.create(CoordinatorRequestContext.TXN_END_TIMEOUT_S + 1, TimeUnit.SECONDS),
                        result);

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);

                assertSame(CoordinatorRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());
            }
        };
    }

    @Test
    void shouldTimeoutForVoteResponse() {
        // Make the server crash when handling the vote request
        server.underlyingActor().getParameters().serverOnVoteResponseCrashProbability = 1.;
        server.underlyingActor().getParameters().serverCanRecover = false;

        new TestKit(system) {
            {
                // testActor() will be the client of the transaction.

                var begin = new TxnBeginMsg();
                var uuid = begin.uuid;
                coordinator.tell(begin, testActor());

                var accept = new TxnAcceptMsg(uuid);
                expectMsg(accept);

                var readMsg = new ReadMsg(uuid, 0);
                coordinator.tell(readMsg, testActor());

                var readResult = new ReadResultMsg(uuid, 0, DatabaseBuilder.DEFAULT_DATA_VALUE);
                expectMsg(readResult);

                var end = new TxnEndMsg(uuid, true);
                coordinator.tell(end, testActor());

                var result = new TxnResultMsg(uuid, false);
                expectMsg(Duration.create(CoordinatorRequestContext.VOTE_RESPONSE_TIMEOUT_S + 1, TimeUnit.SECONDS),
                        result);

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(begin.uuid);

                assertSame(CoordinatorRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());
            }
        };
    }
}
