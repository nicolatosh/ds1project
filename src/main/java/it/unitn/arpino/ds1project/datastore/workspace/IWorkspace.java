package it.unitn.arpino.ds1project.datastore.workspace;

import it.unitn.arpino.ds1project.datastore.database.IDatabase;

import java.util.List;

public interface IWorkspace extends IDatabase {
    List<Integer> getModifiedKeys();
}
