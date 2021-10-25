package it.unitn.arpino.ds1project.nodes;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import it.unitn.arpino.ds1project.messages.Message;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

public abstract class AbstractNode extends AbstractActor {
    protected LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    protected STATUS status;

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
        if (msg instanceof Message) {
            Message message = (Message) msg;

            log.info("received " + message.getType() +
                    "/" + message.getClass().getSimpleName() +
                    " from " + getSender().path().name());
        }

        super.aroundReceive(receive, msg);
    }
}
