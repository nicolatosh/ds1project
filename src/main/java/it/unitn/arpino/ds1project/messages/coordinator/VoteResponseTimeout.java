package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.TimeoutExpired;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.UUID;

/**
 * A message that a {@link Coordinator} can send to itself to signal that the time within which to collect all
 * {@link Server}s' {@link VoteResponse}s has elapsed.
 */
public class VoteResponseTimeout extends TimeoutExpired {
    public VoteResponseTimeout(UUID uuid) {
        super(uuid);
    }
}
