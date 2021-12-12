package it.unitn.arpino.ds1project.simulation;

public class ClientParameters extends Parameters {
    public boolean clientLoop;
    public int numTransactions;
    public int clientMinTxnLength;
    public int clientMaxTxnLength;

    public ClientParameters() {
        clientLoop = Boolean.parseBoolean(cache.getProperty("clientLoop"));
        numTransactions = Integer.parseInt(cache.getProperty("numTransactions"));
        clientMinTxnLength = Integer.parseInt(cache.getProperty("clientMinTxnLength"));
        clientMaxTxnLength = Integer.parseInt(cache.getProperty("clientMaxTxnLength"));
    }
}
