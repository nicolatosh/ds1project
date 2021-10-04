package it.unitn.arpino.ds1project.messages.server;

import akka.actor.ActorRef;

import java.util.List;
import java.util.Map;

/**
 * A message that is used to provide to a server the list of other servers in the Data Store,
 * as well as the initial data items it should write to the database.
 */
public class ServerStartWithKeysMsg extends ServerStartMsg {
    public final List<Integer> keys;
    public final Map<Integer, Integer> versions;
    public final Map<Integer, Integer> values;

    public ServerStartWithKeysMsg(List<ActorRef> servers, List<Integer> keys, Map<Integer, Integer> versions, Map<Integer, Integer> values) {
        super(servers);
        this.keys = List.copyOf(keys);
        this.versions = Map.copyOf(versions);
        this.values = Map.copyOf(values);
    }
}
