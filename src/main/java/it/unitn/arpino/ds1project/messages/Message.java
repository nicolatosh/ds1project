package it.unitn.arpino.ds1project.messages;

import java.io.Serializable;

public abstract class Message implements Serializable {
    public enum TYPE {
        /**
         * Requests from clients to begin or end transactions.
         */
        TxnControl,

        /**
         * Read or Write requests from clients to coordinators.
         */
        Conversational,

        /**
         * Read or Write requests from coordinators to servers, on behalf of clients.
         */
        Internal,

        /**
         * Messages exchanged in the context of the Two-phase commit (2PC) protocol.
         */
        TwoPC,

        /**
         * Messages to control the behavior of nodes at runtime, without a sender.
         */
        NodeControl
    }

    public abstract TYPE getType();
}
