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
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;

public class CoordinatorResumeTest {
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
    void testResumeInConversational() {
        var start = new StartMessage();
        coordinator.tell(start, ActorRef.noSender());

        var begin = new TxnBeginMsg();
        var uuid = begin.uuid;
        coordinator.tell(begin, ActorRef.noSender());

        coordinator.underlyingActor().crash();
        coordinator.underlyingActor().resume();

        var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);

        assertSame(CoordinatorRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
        assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());
    }

    @Test
    void testResumeInStart2PC() {
        new TestKit(system) {
            {
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
                // this forces the coordinator to log START_2PC and crash
                coordinator.underlyingActor().getParameters().coordinatorOnVoteRequestCrashProbability = 1;
                coordinator.tell(end, ActorRef.noSender());

                // restore the normality
                coordinator.underlyingActor().getParameters().coordinatorOnVoteRequestCrashProbability = 0;

                var decision = new FinalDecision(uuid, FinalDecision.Decision.GLOBAL_ABORT);
                expectMsg(Duration.create(coordinator.underlyingActor().getParameters().coordinatorRecoveryTimeS + 1, TimeUnit.SECONDS),
                        decision);

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);

                assertSame(CoordinatorRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());
            }
        };
    }

    @Test
    void testResumeInGlobalCommit() {
        new TestKit(system) {
            {
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
                expectMsg(voteRequest);

                var voteResponse = new VoteResponse(uuid, VoteResponse.Vote.YES);
                // this forces the coordinator to log GLOBAL_COMMIT and crash
                coordinator.underlyingActor().getParameters().coordinatorOnFinalDecisionCrashProbability = 1;
                coordinator.tell(voteResponse, testActor());

                // restore the normality
                coordinator.underlyingActor().getParameters().coordinatorOnFinalDecisionCrashProbability = 0;

                var decision = new FinalDecision(uuid, FinalDecision.Decision.GLOBAL_COMMIT);
                expectMsg(Duration.create(coordinator.underlyingActor().getParameters().coordinatorRecoveryTimeS + 1, TimeUnit.SECONDS),
                        decision);

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);

                assertSame(CoordinatorRequestContext.LogState.GLOBAL_COMMIT, ctx.loggedState());
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.COMMIT, ctx.getProtocolState());
            }
        };
    }

    @Test
    void testResumeInGlobalAbort() {
        new TestKit(system) {
            {
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
                expectMsg(voteRequest);

                var voteResponse = new VoteResponse(uuid, VoteResponse.Vote.NO);
                // this forces the coordinator to log GLOBAL_ABORT and crash
                coordinator.underlyingActor().getParameters().coordinatorOnFinalDecisionCrashProbability = 1;
                coordinator.tell(voteResponse, testActor());

                // restore the normality
                coordinator.underlyingActor().getParameters().coordinatorOnFinalDecisionCrashProbability = 0;

                var decision = new FinalDecision(uuid, FinalDecision.Decision.GLOBAL_ABORT);
                expectMsg(Duration.create(coordinator.underlyingActor().getParameters().coordinatorRecoveryTimeS + 1, TimeUnit.SECONDS),
                        decision);

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);

                assertSame(CoordinatorRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());
            }
        };
    }
}
