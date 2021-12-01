package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnEndMsg;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;

public class CoordinatorOnVoteResponseTest {
    ActorSystem system;
    TestActorRef<Coordinator> coordinator;
    TestKit server0, server1;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        coordinator = TestActorRef.create(system, Coordinator.props());
        server0 = new TestKit(system);
        server1 = new TestKit(system);
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        coordinator = null;
        server0 = null;
    }

    @Test
    void testNoVote() {
        new TestKit(system) {
            {
                var join = new JoinMessage(0, 9);
                coordinator.tell(join, server0.testActor());

                var start = new StartMessage();
                coordinator.tell(start, ActorRef.noSender());

                var begin = new TxnBeginMsg();
                var uuid = begin.uuid;
                coordinator.tell(begin, testActor());

                var accept = new TxnAcceptMsg(uuid);
                expectMsg(accept);

                var readMsg = new ReadMsg(uuid, 0);
                coordinator.tell(readMsg, testActor());

                var readRequest = new ReadRequest(uuid, 0);
                server0.expectMsg(readRequest);

                var end = new TxnEndMsg(uuid, true);
                coordinator.tell(end, testActor());

                var voteRequest = new VoteRequest(uuid);
                server0.expectMsg(voteRequest);

                // server0 casts a NO vote, which makes the coordinator take the final decision

                var noVote = new VoteResponse(uuid, VoteResponse.Vote.NO);
                coordinator.tell(noVote, server0.testActor());

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);

                assertSame(CoordinatorRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());

                // the coordinator should also send the result to the client

                var result = new TxnResultMsg(ctx.uuid, false);
                expectMsg(result);
            }
        };
    }

    @Test
    void testYesVote() {
        new TestKit(system) {
            {
                var join0 = new JoinMessage(0, 9);
                coordinator.tell(join0, server0.testActor());

                var join1 = new JoinMessage(10, 19);
                coordinator.tell(join1, server1.testActor());

                var start = new StartMessage();
                coordinator.tell(start, ActorRef.noSender());

                var begin = new TxnBeginMsg();
                var uuid = begin.uuid;
                coordinator.tell(begin, testActor());

                var accept = new TxnAcceptMsg(uuid);
                expectMsg(accept);

                var readMsg0 = new ReadMsg(uuid, 0);
                coordinator.tell(readMsg0, testActor());

                var readMsg1 = new ReadMsg(uuid, 10);
                coordinator.tell(readMsg1, testActor());

                var readRequest0 = new ReadRequest(uuid, 0);
                server0.expectMsg(readRequest0);

                var readRequest1 = new ReadRequest(uuid, 10);
                server1.expectMsg(readRequest1);

                var end = new TxnEndMsg(uuid, true);
                coordinator.tell(end, testActor());

                var voteRequest = new VoteRequest(uuid);
                server0.expectMsg(voteRequest);
                server1.expectMsg(voteRequest);

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);

                // server0 casts a YES vote, but the coordinator needs one more to take the final decision

                var yesVote = new VoteResponse(uuid, VoteResponse.Vote.YES);

                coordinator.tell(yesVote, server0.testActor());

                assertSame(CoordinatorRequestContext.LogState.START_2PC, ctx.loggedState());
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.WAIT, ctx.getProtocolState());

                // server1 casts a YES vote, which makes the coordinator take the final decision

                coordinator.tell(yesVote, server1.testActor());

                assertSame(CoordinatorRequestContext.LogState.GLOBAL_COMMIT, ctx.loggedState());
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.COMMIT, ctx.getProtocolState());

                // the coordinator should also send the result to the client

                var result = new TxnResultMsg(ctx.uuid, false);
                expectMsg(result);
            }
        };
    }
}
