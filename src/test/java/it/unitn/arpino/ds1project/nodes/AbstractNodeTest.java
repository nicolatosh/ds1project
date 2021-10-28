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

class AbstractNodeTest {
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
                ActorRef testActor = testActor();
                node.tell(new SimpleNode.SimpleMessage(), testActor);
                expectMsg("Message received!");

                node.underlyingActor().crash();
                node.tell(new SimpleNode.SimpleMessage(), testActor);
                expectNoMessage();

                node.underlyingActor().resume();
                node.tell(new SimpleNode.SimpleMessage(), testActor);
                expectMsg("Message received!");
            }
        };
    }

    private static class SimpleNode extends AbstractNode {
        private static class SimpleMessage extends Message {

            @Override
            public TYPE getType() {
                return TYPE.Conversational;
            }
        }

        public static Props props() {
            return Props.create(SimpleNode.class, SimpleNode::new);
        }

        @Override
        public Receive createReceive() {
            return ReceiveBuilder.create()
                    .match(SimpleMessage.class, msg -> getSender().tell("Message received!", getSelf()))
                    .build();
        }
    }
}