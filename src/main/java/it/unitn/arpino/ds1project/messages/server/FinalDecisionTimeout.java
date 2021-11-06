package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.TimeoutExpired;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.UUID;

/**
 * A message that a {@link Server} can send to itself to signal that the time within which to receive the
 * {@link Coordinator}'s {@link FinalDecision} has elapsed.
 */
public class FinalDecisionTimeout extends TimeoutExpired {
    public FinalDecisionTimeout(UUID uuid) {
        super(uuid);
    }
}
