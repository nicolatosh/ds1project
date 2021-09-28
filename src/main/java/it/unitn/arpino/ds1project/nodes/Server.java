package it.unitn.arpino.ds1project.nodes;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.datastore.DataItem;
import it.unitn.arpino.ds1project.datastore.Database;
import it.unitn.arpino.ds1project.datastore.Workspace;
import it.unitn.arpino.ds1project.messages.control.StartMessage;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.twopc.ServerFSM;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.Set;

public class Server extends AbstractActor {
    private Set<ActorRef> servers;

    private final Database database;

    protected Workspace workspace;
    protected ServerFSM twoPcFsm;

    private final ServerStateManager stateManager;

    public Server(Set<Integer> keys) {
        database = new Database(keys);

        stateManager = new ServerStateManager(this);
    }

    public static Props props(Set<Integer> keys) {
        return Props.create(Server.class, () -> new Server(keys));
    }

    @Override
    public Receive createReceive() {
        return new ReceiveBuilder()
                .match(StartMessage.class, this::onGroupMessage)
                .match(ReadRequest.class, this::onReadMsgCoordinator)
                .build();
    }

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
        stateManager.before();
        super.aroundReceive(receive, msg);
    }

    private void onGroupMessage(StartMessage msg) {
        servers.addAll(msg.servers);
    }

    private void onReadMsgCoordinator(ReadRequest request) {
        DataItem item = database.getDataItemByKey(request.key).copy();
        workspace.addDataItem(item);
    }
}
