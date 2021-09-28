package it.unitn.arpino.ds1project.transaction.messages;

/**
 * READ request from the client to the coordinator
 */
public class ReadMsg extends AbstractTxnMessage {
    public final int clientId;
    public final int key;

    /**
     * @param clientId
     * @param key      The key of the value to read
     */
    public ReadMsg(int clientId, int key) {
        this.clientId = clientId;
        this.key = key;
    }
}
