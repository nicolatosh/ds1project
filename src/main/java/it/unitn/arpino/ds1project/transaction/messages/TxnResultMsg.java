package it.unitn.arpino.ds1project.transaction.messages;

/**
 * Message from the coordinator to the client with the outcome of the TXN
 */
public class TxnResultMsg extends AbstractTxnMessage {
    public final Boolean commit;

    /**
     * @param commit If false, the transaction was aborted
     */
    public TxnResultMsg(boolean commit) {
        this.commit = commit;
    }
}
