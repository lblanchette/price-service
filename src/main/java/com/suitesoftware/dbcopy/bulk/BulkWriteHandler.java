package com.suitesoftware.dbcopy.bulk;

import com.suitesoftware.dbcopy.OutParameter;
import org.postgresql.PGConnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * User: lrb
 * Date: 9/24/16
 * Time: 10:43 AM
 * (c) Copyright Suite Business Software
 */
public class BulkWriteHandler {

    private final PgTextInsert client;

    private final DataSource dataSource;

    public static PGConnection getPGConnection(final Connection connection) throws SQLException {
        OutParameter<PGConnection> result = new OutParameter<>();
        if(!tryGetPGConnection(connection, result)) {
            throw new SQLException("Could not obtain a PGConnection");
        }
        return result.get();
    }
    public static boolean tryGetPGConnection(final Connection connection, OutParameter<PGConnection> result) throws SQLException {
        if(tryCastConnection(connection, result)) {
            return true;
        }
        if(tryUnwrapConnection(connection, result)) {
            return true;
        }
        return false;
    }
    private static boolean tryCastConnection(final Connection connection, OutParameter<PGConnection> result) {
        if (connection instanceof PGConnection) {
            result.set((PGConnection) connection);

            return true;
        }
        return false;
    }

    private static boolean tryUnwrapConnection(final Connection connection, OutParameter<PGConnection> result) {
        try {
            if (connection.isWrapperFor(PGConnection.class)) {
                result.set(connection.unwrap(PGConnection.class));
                return true;
            }
            return false;
        } catch(Exception e) {
            return false;
        }
    }


    public BulkWriteHandler(PgTextInsert client, DataSource dataSource) {
        this.client = client;
        this.dataSource = dataSource;
    }

    public void write(List<String []> entities) throws Exception {
        // Obtain a new Connection and execute it in a try with resources block, so it gets closed properly:
        try(Connection connection = dataSource.getConnection()) {
            // Now get the underlying PGConnection for the COPY API wrapping:

            final PGConnection pgConnection = getPGConnection(connection);
            // And finally save all entities by using the COPY API:
            client.saveAll(pgConnection, entities.stream());
            connection.commit();  // gotta in case DS is not autocommit
        }
    }
}
