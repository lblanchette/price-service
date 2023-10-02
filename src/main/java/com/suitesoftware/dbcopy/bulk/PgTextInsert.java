package com.suitesoftware.dbcopy.bulk;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.PGCopyOutputStream;

import java.sql.SQLException;
import java.util.stream.Stream;

/**
 * User: lrb
 * Date: 9/24/16
 * Time: 10:51 AM
 * (c) Copyright Suite Business Software
 */
public class PgTextInsert {

    String schemaName;
    String tableName;

    private String [] columns;

    public PgTextInsert(String schemaName, String tableName, String[] columns)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = columns;
    }

    public void saveAll(PGConnection connection, Stream<String []> entities) throws SQLException {

        CopyManager cpManager = connection.getCopyAPI();
        CopyIn copyIn = cpManager.copyIn(getCopyCommand());

        try (PgTextWriter bw = new PgTextWriter()) {

            // Wrap the CopyOutputStream in our own Writer:
            bw.open(new PGCopyOutputStream(copyIn));

            // Insert Each Column:
            entities.forEach(entity -> this.saveEntity(bw, entity));

            bw.write("\\.\n");

        }
    }

    private void saveEntity(PgTextWriter bw, String [] entity) throws BulkCopyFailedException {

        synchronized (bw) {

            // Start a New Row:
            //bw.startRow(columns.size());

            boolean first = true;
            // Iterate over each column mapping:
            for(String field : entity) {
                try {
                    if(!first) {
                        bw.write("|");
                    } else {
                        first = false;
                    }
                    String escField;
                    if(field == null) {
                        escField = "\\N";
                        //escField = "";
                    } else {
                        escField = field.replaceAll("\\\\", "\\\\\\\\");
                        escField = escField.replaceAll("\\|", "\\\\|");
                        escField = escField.replaceAll("\n", "\\\\n");
                        escField = escField.replaceAll("\r", "\\\\r");
                    }
                    bw.write(escField);
                } catch (Exception e) {
                    throw new BulkCopyFailedException(e);
                }
            }
            bw.write("\n");
        }
    }

    public String getFullQualifiedTableName()
    {
      if (schemaName == null || schemaName.trim().length() == 0) {
        return this.tableName;
      }
      return String.format("%1$s.%2$s", new Object[] { schemaName, tableName });
    }


    private String getCopyCommand()
    {
//        return String.format("COPY %1$s(%2$s) FROM STDIN BINARY",
//                table.GetFullQualifiedTableName(),
//                commaSeparatedColumns);
        return String.format("COPY %1$s(%2$s) FROM STDIN WITH DELIMITER '|'",
                getFullQualifiedTableName(),
                "\"" + String.join("\",\"",columns) + "\"");

    }


}
