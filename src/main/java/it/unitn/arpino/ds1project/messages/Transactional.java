package it.unitn.arpino.ds1project.messages;

import java.util.UUID;

public interface Transactional {
    /**
     * @return The identifier of the transaction related to this message.
     */
    UUID uuid();
}
