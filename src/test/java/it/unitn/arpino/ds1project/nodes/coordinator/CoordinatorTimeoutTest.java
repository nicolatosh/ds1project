package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnEndMsg;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext;
import org.junit.jupiter.api.*;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CoordinatorTimeoutTest {
    ActorSystem system;
    TestActorRef<Coordinator> coordinator;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        coordinator = TestActorRef.create(system, Coordinator.props());
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        coordinator = null;
    }

    @Test
    @Order(1)
    void testOnTxnEndTimeout() {
        new TestKit(system) {
            {
                // pass the probe as a server to the coordinator
                var join = new JoinMessage(0, 9);
                coordinator.tell(join, testActor());

                var start = new StartMessage();
                coordinator.tell(start, ActorRef.noSender());

                var begin = new TxnBeginMsg();
                coordinator.tell(begin, ActorRef.noSender());

                var readMsg = new ReadMsg(begin.uuid, 0);
                coordinator.tell(readMsg, ActorRef.noSender());

                var readRequest = new ReadRequest(begin.uuid, 0);
                expectMsg(readRequest);

                var decision = new FinalDecision(begin.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                expectMsg(Duration.create(CoordinatorRequestContext.TXN_END_TIMEOUT_S + 1, TimeUnit.SECONDS),
                        decision);

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(begin.uuid);

                assertSame(CoordinatorRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());
            }
        };
    }

    @Test
    @Order(2)
    void testOnVoteResponseTimeout() {
        new TestKit(system) {
            {
                // pass the probe as a server to the coordinator
                var join = new JoinMessage(0, 9);
                coordinator.tell(join, testActor());

                var start = new StartMessage();
                coordinator.tell(start, ActorRef.noSender());

                var begin = new TxnBeginMsg();
                var uuid = begin.uuid;
                coordinator.tell(begin, ActorRef.noSender());

                var readMsg = new ReadMsg(uuid, 0);
                coordinator.tell(readMsg, ActorRef.noSender());

                var readRequest = new ReadRequest(uuid, 0);
                expectMsg(readRequest);

                var end = new TxnEndMsg(uuid, true);
                coordinator.tell(end, ActorRef.noSender());

                var voteRequest = new VoteRequest(uuid);
                expectMsg(Duration.create(ServerRequestContext.VOTE_REQUEST_TIMEOUT_S + 1, TimeUnit.SECONDS),
                        voteRequest);

                var decision = new FinalDecision(begin.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                expectMsg(Duration.create(CoordinatorRequestContext.VOTE_RESPONSE_TIMEOUT_S + 1, TimeUnit.SECONDS),
                        decision);

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(begin.uuid);

                assertSame(CoordinatorRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());
            }
        };
    }
}
