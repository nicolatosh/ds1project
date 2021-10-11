package it.unitn.arpino.ds1project.datastore;

import java.util.List;

public interface ITransaction {
    IWorkspace getWorkspace();

    List<Lock> getLocks();
}
