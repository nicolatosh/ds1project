package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.MessageType;
import it.unitn.arpino.ds1project.twopc.Decision;

import java.io.Serializable;

public class DecisionResponse implements MessageType, Serializable {
    public final Decision decision;

    public DecisionResponse(Decision decision) {
        this.decision = decision;
    }

    @Override
    public TYPE getType() {
        return TYPE.TwoPC;
    }
}
