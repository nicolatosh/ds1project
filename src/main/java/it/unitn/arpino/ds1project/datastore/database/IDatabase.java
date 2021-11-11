package it.unitn.arpino.ds1project.datastore.database;

import it.unitn.arpino.ds1project.datastore.workspace.IWorkspace;

import java.util.Map;

/**
 * Interface to a generic key-value database, where keys and values are integers. Access to the database must be
 * sequential: it does not support concurrent access.
 */
public interface IDatabase {
    /**
     * Preloads the database with the specified keys and values.
     *
     * @param keyValues Map of keys and vales which should be copied in the database.
     */
    void initialize(Map<Integer, Integer> keyValues);

    /**
     * Reads the data item in the database with the specified key.
     *
     * @param key Key of the data item which should be read.
     * @return The read value.
     */
    Integer read(int key);

    /**
     * Writes a value to the data item in the database with the specified key.
     *
     * @param key   The key of the data item to write.
     * @param value The value to write into the data item.
     */
    void write(int key, int value);

    /**
     * @param key Key of the data item of which to retrieve the version.
     * @return The version of the data item with the specified key.
     */
    Integer getVersion(int key);


    /**
     * Sets the version of a data item.
     *
     * @param key     Key of the data item of which to set the version.
     * @param version Version to which to set the data item.
     */
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

    IDatabase copy();
}
