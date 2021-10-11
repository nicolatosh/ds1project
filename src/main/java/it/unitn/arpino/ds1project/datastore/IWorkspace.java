package it.unitn.arpino.ds1project.datastore;

import java.util.List;

public interface IWorkspace extends IDatabase {
    List<Integer> getModifiedKeys();
}
