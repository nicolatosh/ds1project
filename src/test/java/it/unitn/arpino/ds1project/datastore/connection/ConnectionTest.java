package it.unitn.arpino.ds1project.datastore.connection;

import it.unitn.arpino.ds1project.datastore.controller.IDatabaseController;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.datastore.database.IDatabase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertSame;

class ConnectionTest {
    IDatabase database;
    IDatabaseController controller;
    IDatabase copy;

    @BeforeEach
    void setUp() {
        DatabaseBuilder builder = DatabaseBuilder.newBuilder().create();
        database = builder.getDatabase();
        controller = builder.getController();
        copy = database.copy();
    }

    @Test
    void read() {
        IConnection connection = controller.beginTransaction();

        IntStream.range(0, 10).forEach(i -> Assertions.assertEquals(DatabaseBuilder.DEFAULT_DATA_VALUE, connection.read(i)));
    }

    @Test
    void readYourWrites() {
        IConnection connection = controller.beginTransaction();

        connection.write(4, 40);
        Assertions.assertEquals(40, connection.read(4));

        connection.write(7, 70);
        Assertions.assertEquals(70, connection.read(7));
    }

    @Test
    void prepare_NonInterferingTransactions() {
        IConnection first = controller.beginTransaction();
        IConnection second = controller.beginTransaction();

        // Concurrent interleaving, reading different keys
        first.write(4, 40);
        second.write(8, 80);
        first.write(7, 70);
        second.write(3, 30);

        // both transactions should be preparable
        assertSame(first.prepare(), IDatabaseController.Response.PREPARED);
        assertSame(second.prepare(), IDatabaseController.Response.PREPARED);
    }

    @Test
    void prepare_InterferingTransactions() {
        IConnection first = controller.beginTransaction();
        IConnection second = controller.beginTransaction();

        // Common keys are: 5, 6

        first.write(4, 40);
        first.write(5, 50);
        first.write(6, 60);
        first.write(7, 70);

        second.write(3, 30);
        second.write(5, 500);
        second.write(6, 600);
        second.write(8, 80);

        // only one should be preparable
        assertSame(first.prepare(), IDatabaseController.Response.PREPARED);
        assertSame(second.prepare(), IDatabaseController.Response.ABORT);
    }

    @Test
    void commit() {
        IConnection connection = controller.beginTransaction();

        connection.write(4, 40);
        connection.write(7, 70);

        connection.prepare();
        connection.commit();

        copy.write(4, 40);
        copy.setVersion(4, 1);
        copy.write(7, 70);
        copy.setVersion(7, 1);

        Assertions.assertEquals(copy, database);
    }

    @Test
    void abort() {
        IConnection connection = controller.beginTransaction();

        connection.write(4, 40);
        connection.write(7, 70);

        connection.prepare();
        connection.abort();

        // database remains unmodified
        Assertions.assertEquals(copy, database);
    }
}