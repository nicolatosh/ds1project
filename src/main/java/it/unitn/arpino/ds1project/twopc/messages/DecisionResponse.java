package it.unitn.arpino.ds1project.twopc.messages;

import it.unitn.arpino.ds1project.twopc.Decision;

import java.io.Serializable;

public class DecisionResponse implements Serializable {
    public final Decision decision;

    public DecisionResponse(Decision decision) {
        this.decision = decision;
    }
}
