package it.unitn.arpino.ds1project.messages;

public interface MessageType {
    enum TYPE {
        TxnControl,
        Conversational,
        Internal,
        TwoPC,
        NodeControl
    }

    TYPE getType();
}
