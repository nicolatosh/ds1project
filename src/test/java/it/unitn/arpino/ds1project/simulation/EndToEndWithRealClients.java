package it.unitn.arpino.ds1project.simulation;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.messages.client.CoordinatorList;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.coordinator.CoordinatorRequestContext;
import it.unitn.arpino.ds1project.nodes.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class EndToEndWithRealClients {
    ActorSystem system;
    List<TestActorRef<TxnClient>> clients;
    List<TestActorRef<Coordinator>> coordinators;
    List<TestActorRef<Server>> servers;

    int initialSum;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create("e2eWithClients");

        clients = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TestActorRef<TxnClient> client = TestActorRef.create(system, TxnClient.props(), "client" + i);
            client.underlyingActor().getParameters().clientLoop = false;
            client.underlyingActor().getParameters().numTransactions = 4;
            client.underlyingActor().getParameters().clientMinTxnLength = 10;
            client.underlyingActor().getParameters().clientMaxTxnLength = 20;
            clients.add(client);
        }

        coordinators = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TestActorRef<Coordinator> coord = TestActorRef.create(system, Coordinator.props(), "coord" + i);
            coord.underlyingActor().getParameters().coordinatorOnVoteRequestSuccessProbability = 0.9;
            coord.underlyingActor().getParameters().coordinatorOnFinalDecisionSuccessProbability = 0.9;
            coord.underlyingActor().getParameters().coordinatorRecoveryTimeMs = 2000;
            coordinators.add(coord);
        }

        servers = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            TestActorRef<Server> server = TestActorRef.create(system, Server.props(i * 10, i * 10 + 9), "server" + i);
            server.underlyingActor().getParameters().serverOnVoteResponseSuccessProbability = 0.9;
            server.underlyingActor().getParameters().serverOnDecisionRequestSuccessProbability = 0.9;
            server.underlyingActor().getParameters().serverOnDecisionResponseSuccessProbability = 0.9;
            server.underlyingActor().getParameters().serverRecoveryTimeMs = 2000;

            servers.add(server);
        }

        initialSum = calculateSum();

        for (int i = 0; i < servers.size(); i++) {
            var server = servers.get(i);

            var join = new JoinMessage(i * 10, i * 10 + 9);
            coordinators.forEach(coordinator -> coordinator.tell(join, server));
        }

        int maxKey = (servers.size() - 1) * 10 + 9;
        var coordinatorList = new CoordinatorList(

                coordinators.stream()
                        .map(actorRef -> actorRef.underlyingActor().getSelf())
                        .collect(Collectors.toList()),

                maxKey);
        clients.forEach(client -> client.tell(coordinatorList, ActorRef.noSender()));

        var start = new StartMessage();
        coordinators.forEach(coordinator -> coordinator.tell(start, ActorRef.noSender()));
    }

    @AfterEach
    void tearDown() {
        system = null;
        clients = null;
        coordinators = null;
        servers = null;
    }

    private int calculateSum() {
        int currentSum = 0;
        for (int i = 0; i < servers.size(); i++) {

            var database = servers.get(i).underlyingActor().getDatabase();
            for (int key = i * 10; key <= i * 10 + 9; key++) {
                currentSum += database.read(key);
            }
        }

        return currentSum;
    }

    @Test
    void e2eWithClients() {
        var start = new StartMessage();
        clients.forEach(client -> client.tell(start, ActorRef.noSender()));

        new TestKit(system) {
            {
                // clients are done
                while (true) {
                    try {
                        awaitCond(() -> clients.stream()
                                        .map(TestActorRef::underlyingActor)
                                        .allMatch(client -> client.getNumTransactionsLeft() == 0),
                                Duration.create(20, TimeUnit.SECONDS),
                                Duration.create(5, TimeUnit.SECONDS),
                                null);
                    } catch (AssertionError ignored) {
                        continue;
                    }
                    break;
                }

                // done messages
                while (true) {
                    try {
                        awaitCond(() -> coordinators.stream()
                                        .map(TestActorRef::underlyingActor)
                                        .allMatch(coord -> coord.getRepository()
                                                .getAllRequestContexts().stream()
                                                .allMatch(CoordinatorRequestContext::allParticipantsDone)),
                                Duration.create(20, TimeUnit.SECONDS),
                                Duration.create(5, TimeUnit.SECONDS),
                                null);
                    } catch (AssertionError ignored) {
                        continue;
                    }
                    break;
                }
            }
        };

        System.out.println("Checking consistency...");

        int actualSum = calculateSum();

        Assertions.assertEquals(initialSum, actualSum);
    }
}
