package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
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

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;

public class CoordinatorResumeTest {
    ActorSystem system;
    TestActorRef<SelfHealingCoordinator> coordinator;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        coordinator = TestActorRef.create(system, SelfHealingCoordinator.props());
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

        var uuid = UUID.randomUUID();
        var begin = new TxnBeginMsg(uuid);
        coordinator.tell(begin, ActorRef.noSender());

        coordinator.underlyingActor().crash();
        coordinator.underlyingActor().resume();

        var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);

        assertSame(CoordinatorRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
    }

    @Test
    void testResumeInStart2PC() {
        new TestKit(system) {
            {
                // Description of the test:
                // The client reads a data item, requests to commit.
                // The server receives the request to commit, crashes while sending the vote request.
                // Upon resuming, it should send the final decision (abort) to the server.


                // this forces the coordinator to log START_2PC and crash while sending the vote request.
                coordinator.underlyingActor().getParameters().coordinatorOnVoteRequestSuccessProbability = 0;


                var join = new JoinMessage(0, 9);
                coordinator.tell(join, testActor());

                var start = new StartMessage();
                coordinator.tell(start, ActorRef.noSender());

                var uuid = UUID.randomUUID();
                var begin = new TxnBeginMsg(uuid);
                coordinator.tell(begin, ActorRef.noSender());

                var readMsg = new ReadMsg(uuid, 0);
                coordinator.tell(readMsg, ActorRef.noSender());

                var readRequest = new ReadRequest(uuid, 0);
                expectMsg(readRequest);

                var end = new TxnEndMsg(uuid, true);
                coordinator.tell(end, ActorRef.noSender());

                var decision = new FinalDecision(uuid, FinalDecision.Decision.GLOBAL_ABORT);
                expectMsg(Duration.create(coordinator.underlyingActor().getParameters().coordinatorRecoveryTimeMs + 1000, TimeUnit.SECONDS),
                        decision);

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);
                assertSame(CoordinatorRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
            }
        };
    }

    @Test
    void testResumeInGlobalCommit() {
        new TestKit(system) {
            {
                // Description of the test:
                // The client reads a data item, requests to commit.
                // The server requests the votes, collects the vote (YES), writes the final decision in the local log (commit),
                // and crashes before sending the final decision.
                // Upon resuming, it should send the final decision (commit) to the server.


                // this forces the coordinator to log GLOBAL_COMMIT after collecting the vote responses and crash.
                coordinator.underlyingActor().getParameters().coordinatorOnFinalDecisionSuccessProbability = 0;


                var join = new JoinMessage(0, 9);
                coordinator.tell(join, testActor());

                var start = new StartMessage();
                coordinator.tell(start, ActorRef.noSender());

                var uuid = UUID.randomUUID();
                var begin = new TxnBeginMsg(uuid);
                coordinator.tell(begin, ActorRef.noSender());

                var readMsg = new ReadMsg(uuid, 0);
                coordinator.tell(readMsg, ActorRef.noSender());

                var readRequest = new ReadRequest(uuid, 0);
                expectMsg(readRequest);

                var end = new TxnEndMsg(uuid, true);
                coordinator.tell(end, ActorRef.noSender());

                var voteRequest = new VoteRequest(uuid, Set.of(testActor()));
                expectMsg(voteRequest);

                var voteResponse = new VoteResponse(uuid, VoteResponse.Vote.YES);
                coordinator.tell(voteResponse, testActor());

                var decision = new FinalDecision(uuid, FinalDecision.Decision.GLOBAL_COMMIT);
                expectMsg(Duration.create(coordinator.underlyingActor().getParameters().coordinatorRecoveryTimeMs + 1000, TimeUnit.SECONDS),
                        decision);

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);
                assertSame(CoordinatorRequestContext.LogState.GLOBAL_COMMIT, ctx.loggedState());
            }
        };
    }

    @Test
    void testResumeInGlobalAbort() {
        new TestKit(system) {
            {
                // Description of the test:
                // The client reads a data item, requests to commit.
                // The server requests the votes, collects the vote (NO), writes the final decision in the local log (abort),
                // and crashes before sending the final decision.
                // Upon resuming, it should send the final decision (abort) to the server.


                // this forces the coordinator to log GLOBAL_ABORT and crash
                coordinator.underlyingActor().getParameters().coordinatorOnFinalDecisionSuccessProbability = 0;


                var join = new JoinMessage(0, 9);
                coordinator.tell(join, testActor());

                var start = new StartMessage();
                coordinator.tell(start, ActorRef.noSender());

                var uuid = UUID.randomUUID();
                var begin = new TxnBeginMsg(uuid);
                coordinator.tell(begin, ActorRef.noSender());

                var readMsg = new ReadMsg(uuid, 0);
                coordinator.tell(readMsg, ActorRef.noSender());

                var readRequest = new ReadRequest(uuid, 0);
                expectMsg(readRequest);

                var end = new TxnEndMsg(uuid, true);
                coordinator.tell(end, ActorRef.noSender());

                var voteRequest = new VoteRequest(uuid, Set.of(testActor()));
                expectMsg(voteRequest);

                var voteResponse = new VoteResponse(uuid, VoteResponse.Vote.NO);
                coordinator.tell(voteResponse, testActor());

                // the "NO" vote from the participant counted as a Done message; thus, while resuming, the coordinator
                // does not need to send the final decision.
                // var decision = new FinalDecision(uuid, FinalDecision.Decision.GLOBAL_ABORT);
                // expectMsg(Duration.create(coordinator.underlyingActor().getParameters().coordinatorRecoveryTimeMs + 1000, TimeUnit.SECONDS),
                //        decision);

                var ctx = coordinator.underlyingActor().getRepository().getRequestContextById(uuid);
                assertSame(CoordinatorRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
            }
        };
    }
}

class SelfHealingCoordinator extends Coordinator {
    public static Props props() {
        return Props.create(SelfHealingCoordinator.class, SelfHealingCoordinator::new);
    }

    @Override
    protected void crash() {
        getParameters().coordinatorOnVoteRequestSuccessProbability = 1;
        getParameters().coordinatorOnFinalDecisionSuccessProbability = 1;
        super.crash();
    }
}
