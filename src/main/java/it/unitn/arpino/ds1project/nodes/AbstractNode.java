package it.unitn.arpino.ds1project.nodes;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import it.unitn.arpino.ds1project.messages.Message;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public abstract class AbstractNode extends AbstractActor {
    protected LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    @Override
    public void aroundPreStart() {
        super.aroundPreStart();
        getContext().setReceiveTimeout(Duration.ofSeconds(10));
    }

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
        if (msg instanceof Message) {
            Message message = (Message) msg;

            if (message.getType() != Message.TYPE.NodeControl) {
                delay();
            }

            log.info("received " + message.getType() +
                    "/" + message.getClass().getSimpleName() +
                    " from " + getSender().path().name());
        }

        super.aroundReceive(receive, msg);
    }

    private void delay() {
        try {
            TimeUnit.SECONDS.sleep(0);
        } catch (InterruptedException ignored) {
        }
    }
}
