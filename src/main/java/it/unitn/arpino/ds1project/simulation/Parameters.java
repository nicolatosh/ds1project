package it.unitn.arpino.ds1project.simulation;

import it.unitn.arpino.ds1project.nodes.DataStoreNode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class Parameters {
    protected static final Properties cache;

    static {
        cache = new Properties();

        try (InputStream file = DataStoreNode.class.getResourceAsStream("/simulation.properties")) {
            if (file == null) {
                throw new FileNotFoundException("simulation.properties not found");
            }
            cache.load(file);

        } catch (IOException ignored) {
        }
    }
}
