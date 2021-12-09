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

public class SingleActorThreadingTest {
    @Test
    void threadSleepBlocksTheActorScheduler() {
        // description of the test:
        // this test aims at demonstrating that Thread.sleep blocks an actor's scheduler
        // (hence, that the actor and its scheduler share the same thread).

        // 1. The testkit tells the Node "start".
        // 2. The Node schedules a "timeout" message to itself, to be sent in two seconds.
        // When the Node will receive the "timeout" message, it will send a "done" message to the probe.
        // 3. The testkit tells the Node "sleep". (the scheduler of point 2 should still be running).
        // 4. The Node calls Thread.sleep with a duration of 4 seconds.
        // Initially, we assumed that the Node scheduler would still run in the background, despite the Thread.sleep.
        // We expected the following output:
        // start -> sleep start -> timeout -> done -> sleep end -> timeout
        // However, we observed the following output:
        // start -> sleep start -> sleep end -> timeout -> done
        // Thanks to this, we discovered that Thread.sleep "blocks" the whole actor, including its scheduler.

        var system = ActorSystem.create("test");

        new TestKit(system) {
            {
                var node = system.actorOf(Actor.props(testActor()), "node");

                node.tell("start", testActor());
                node.tell("sleep", testActor());

                expectMsg(scala.concurrent.duration.Duration.create(5, TimeUnit.SECONDS), "done");
                System.out.println("done");
            }
        };
    }

    static class Actor extends AbstractActor {
        private final ActorRef probe;

        public static Props props(ActorRef probe) {
            return Props.create(Actor.class, () -> new Actor(probe));
        }

        public Actor(ActorRef probe) {
            this.probe = probe;
        }

        @Override
        public Receive createReceive() {
            return new ReceiveBuilder()
                    // the following functions are used by the "threadSleepBlocksTheActorScheduler" test
                    .matchEquals("start", msg -> {
                        System.out.println("start");

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
}

