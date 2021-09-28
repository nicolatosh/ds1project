package it.unitn.arpino.ds1project.twopc;

public class CoordinatorFSM {
    public enum STATE {
        INIT,
        WAIT,
        ABORT,
        COMMIT
    }

    private STATE state;

    public CoordinatorFSM() {
        this.state = STATE.INIT;
    }

    public STATE getState() {
        return state;
    }

    public void setState(STATE state) {
        this.state = state;
    }
}
