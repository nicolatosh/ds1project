package it.unitn.arpino.ds1project.datastore.workspace;

import it.unitn.arpino.ds1project.datastore.database.Database;

import java.util.ArrayList;
import java.util.List;

public class Workspace extends Database implements IWorkspace {
    @Override
    public List<Integer> getModifiedKeys() {
        return new ArrayList<>(values.keySet());
    }
}
