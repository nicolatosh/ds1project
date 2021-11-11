package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

class DataStoreNodeTest {
    ActorSystem system;
    TestActorRef<SimpleNode> node;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        node = TestActorRef.create(system, SimpleNode.props());
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, scala.concurrent.duration.Duration.create(1, TimeUnit.SECONDS), false);
        node = null;
    }

    @Test
    void crashAndResume() {
        new TestKit(system) {
            {
                node.tell("Hello", testActor());
                expectMsg("World");

                node.underlyingActor().crash();
                node.tell("Hello", testActor());
                expectNoMessage();

                node.underlyingActor().resume();
                node.tell("Hello", testActor());
                expectMsg("World");
            }
        };
    }


    @SuppressWarnings("rawtypes")
    private static class SimpleNode extends DataStoreNode {
        public static Props props() {
            return Props.create(SimpleNode.class, SimpleNode::new);
        }

        @Override
        public Receive createReceive() {
            return ReceiveBuilder.create()
                    .matchEquals("Hello", msg -> getSender().tell("World", getSelf()))
                    .build();
        }
    }
}