package it.unitn.arpino.ds1project.twopc;

public class ServerFSM {
    public enum STATE {
        INIT,
        READY,
        ABORT,
        COMMIT
    }


    private CoordinatorFSM.STATE state;

    public ServerFSM() {
        this.state = CoordinatorFSM.STATE.INIT;
    }

    public CoordinatorFSM.STATE getState() {
        return state;
    }

    public void setState(CoordinatorFSM.STATE state) {
        this.state = state;
    }
}
