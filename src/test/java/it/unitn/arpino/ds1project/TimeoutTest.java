package it.unitn.arpino.ds1project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.testkit.TestKit;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class TimeoutTest {
    @Test
    void name() {
        var system = ActorSystem.create("ds1project");

        new TestKit(system) {
            {
                var node = system.actorOf(Node.props(testActor()), "node");

                node.tell("timer", testActor());
                node.tell("sleep", testActor());

                expectMsg(scala.concurrent.duration.Duration.create(5, TimeUnit.SECONDS), "done");
            }
        };
    }
}

class Node extends AbstractActor {
    private ActorRef probe;

    public static Props props(ActorRef probe) {
        return Props.create(Node.class, () -> new Node(probe));
    }

    public Node(ActorRef probe) {
        this.probe = probe;
    }

    @Override
    public Receive createReceive() {
        return new ReceiveBuilder()
                .matchEquals("timer", msg -> {
                    System.out.println("timer start");

                    getContext().getSystem().scheduler().scheduleOnce(
                            Duration.ofSeconds(2), // delay
                            getSelf(), // receiver
                            "timeout", // message
                            getContext().dispatcher(), // executor
                            getSelf()); // sender
                })
                .matchEquals("sleep", msg -> {
                    System.out.println("sleep start");
                    Thread.sleep(4000);
                    System.out.println("sleep end");
                })
                .matchEquals("timeout", msg -> {
                    System.out.println("timeout");
                    probe.tell("done", getSelf());
                })
                .build();
    }
}
