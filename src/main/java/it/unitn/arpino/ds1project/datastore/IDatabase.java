package it.unitn.arpino.ds1project.datastore;

import java.util.Map;

public interface IDatabase {

    void initializeDb(Map<Integer, Integer> keyvalues);

    Integer read(int key);

    void write(int key, int value);

    Integer getVersion(int key);

    void setVersion(int key, int version);

    /**
     * Verifies that the items in the workspace can be written into the database without violating serializability.
     * This holds when the versions of the items in the workspace are the same of those in the database,
     * and does not if another transaction has already committed in the meanwhile, updating the versions of the items
     * in the database.
     *
     * @return true if the items in the workspace can be written into the database without violating serializability,
     * false otherwise.
     */
    boolean validate(IWorkspace workspace);
}
