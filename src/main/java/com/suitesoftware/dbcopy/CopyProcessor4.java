package com.suitesoftware.dbcopy;

import com.suitesoftware.dbcopy.bulk.BulkProcessor;
import com.suitesoftware.dbcopy.bulk.BulkWriteHandler;
import com.suitesoftware.dbcopy.bulk.PgTextInsert;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterUtils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
/**
 * This utility is used large batch type transfers between DBs.  It does a "copy" using SQL to read and
 * BulkProcessor to write. The configuration class CopyDef defines source and target.  Source can be a
 * table or select statement.  Target can be a table or temporary table.  ColCopyDef specified columns
 * and data types on both sides. DdlDef is used what a temp table is required.
 *
 * User: lrb
 * Date: Oct 2, 2010
 * Time: 1:08:38 AM
 * (c) Copyright Suite Business Software, ALl rights reserved
 */
public class CopyProcessor4 {

    private class Stats {
        long startTime = System.currentTimeMillis();
        long queryTime = 0;
        long rowsProcessed = 0;
        long intervalRowsProcessed = 0;
    }

    private final Logger log = Logger.getLogger(getClass());
    DataSource srcDataSource;
    DataSource dstDataSource;

    public DataSource getSrcDataSource() {
        return srcDataSource;
    }
    public void setSrcDataSource(DataSource srcDataSource) {
        this.srcDataSource = srcDataSource;
    }

    public DataSource getDstDataSource() {
        return dstDataSource;
    }

    public void setDstDataSource(DataSource dstDataSource) {
        this.dstDataSource = dstDataSource;
    }

    // define fields for gatering stats

    private final int PAGE_SIZE = 10000;

    String buildSelect(CopyDef copyDef) {
        if(copyDef.getSelectSql() != null) {
            return copyDef.getSelectSql();
        }
        final StringBuilder select = new StringBuilder("select ");
        String sortKeyName = null;
        boolean first = true;
        for(ColCopyDef colDef : copyDef.columns) {
            if(!first) {
                select.append(",");
            }
            first = false;
            select.append(colDef.getName());
            if(colDef.isSortKey()) {
                sortKeyName = colDef.getName();
            }

        }
        StringBuilder from = new StringBuilder(" FROM ");
        from.append(copyDef.getSrcTable()).append(" ");
        if(copyDef.getWhereClause() != null && copyDef.getWhereClause().length() > 0) {
            from.append(" WHERE ").append(copyDef.getWhereClause());
        }
        if(sortKeyName != null && sortKeyName.length() > 0) {
            from.append(" ORDER BY ").append(sortKeyName).append(" ASC");
        }
        return select.append(from).toString();
    }

    private String tempTableName(String dstTableName) {
        return dstTableName + "_TMP";
    }

    private void createTemp(String dstTableName, String createDdl)  throws SQLException {
        StringBuilder sbCreate = new StringBuilder();
        sbCreate.append("BEGIN;\n");
        sbCreate.append("DROP TABLE if exists ").append(tempTableName(dstTableName)).append(";\n");
        sbCreate.append("create table  ").append(tempTableName(dstTableName)).append(" ").append(createDdl).append(";\n");
        sbCreate.append("COMMIT;\n");
        sbCreate.append("END;\n");
        log.info("Begin processing " + tempTableName(dstTableName) + "\n" + sbCreate);
        try (Connection conn = dstDataSource.getConnection()) {
            conn.createStatement().execute(sbCreate.toString());
            if(!conn.getAutoCommit()) {
                conn.commit();
            }
        }
    }

    private void initDstTable(CopyDef copyDef, MapSqlParameterSource msps) throws SQLException {
        if(copyDef.isUseTemp()) {
            createTemp(copyDef.getDstTable(), copyDef.getDdlDef().getCreateDdl());
        }
        if(copyDef.getPrepSql() != null && copyDef.getPrepSql().trim().length() > 0) {
            try (Connection conn = dstDataSource.getConnection()) {

                makeNamedParameterStatement(conn, copyDef.getPrepSql(), msps).execute();

                if(!conn.getAutoCommit()) {
                    conn.commit();
                }
            }
        }
    }

    private void swapTemp(CopyDef copyDef) throws SQLException {

        log.info("Start SWAP");
        StringBuilder sbAlter = new StringBuilder();
        sbAlter.append("BEGIN;\n");
        sbAlter.append("DROP TABLE if exists ").append(copyDef.getDstTable()).append(";\n");
        sbAlter.append("ALTER TABLE  ").append(tempTableName(copyDef.getDstTable())).append(" RENAME TO ").append(copyDef.getDstTable()).append(";\n");
        sbAlter.append("ALTER TABLE  ").append(copyDef.getDstTable())
                .append(" ADD CONSTRAINT  ").append(copyDef.getDstTable()).append("_PKEY PRIMARY KEY(")
                .append(copyDef.getDdlDef().getPrimaryKey())
                .append(");\n");
        sbAlter.append("COMMIT;\n");
        sbAlter.append("END;\n");
        log.debug(sbAlter.toString());
        try (Connection conn = dstDataSource.getConnection()) {
            conn.createStatement().execute(sbAlter.toString());
            if(!conn.getAutoCommit()) {
                conn.commit();
            }
        }
        if (copyDef.getDdlDef().indexes != null) {
            for (String index : copyDef.getDdlDef().indexes) {
                try (Connection conn = dstDataSource.getConnection()) {
                    conn.createStatement().execute(index);
                    if(!conn.getAutoCommit()) {
                        conn.commit();
                    }
                }
            }
        }
        log.info("SWAP complete");
    }

    private void postProcessTable(CopyDef copyDef, MapSqlParameterSource msps)  throws SQLException {
        if(copyDef.isUseTemp()) {
            swapTemp(copyDef);
        }
    }

    private PreparedStatement makeNamedParameterStatement(Connection conn, String sql, MapSqlParameterSource msps) throws SQLException {
        Object[] values = NamedParameterUtils.buildValueArray(sql, msps.getValues());
        String pSql = NamedParameterUtils.substituteNamedParameters(sql, msps);
        PreparedStatement ps = conn.prepareStatement(pSql);
        for (int i = 0; i < values.length; i++) {
            ps.setObject(i + 1, values[i]);
        }
        return ps;
    }

    void serialBulkLoadTable(final CopyDef copyDef, MapSqlParameterSource msps) {

        try {
            log.info("Start bulkLoadTable(): " + copyDef.getSrcTable());

            String select = buildSelect(copyDef);

            initDstTable(copyDef, msps);

            boolean completedNoError = false;

            String loadTableName = copyDef.isUseTemp() ? tempTableName(copyDef.getDstTable()) : copyDef.getDstTable();

            final Stats stats = new Stats();

            try( BulkProcessor bulkProcessor = new BulkProcessor(new BulkWriteHandler(new PgTextInsert("public",loadTableName,copyDef.getDstColumnNames()), getDstDataSource()), PAGE_SIZE)) {

                try(Connection conn = getSrcDataSource().getConnection()) {

                    stats.startTime = System.currentTimeMillis();

                    ResultSet resultSet = makeNamedParameterStatement(conn, select, msps).executeQuery();

                    while (resultSet.next()) {

                        if(stats.rowsProcessed == 0) {
                            stats.queryTime = System.currentTimeMillis() - stats.startTime;
                            log.info("Fetch Stats  " + copyDef.getSrcTable() + ", query time: " + (stats.queryTime / 1000)+ "s");
                        }

                        String[] row = new String[copyDef.columns.length];

                        for (int i = 0; i < copyDef.columns.length; i++) {
                            ColCopyDef colCopyDef = copyDef.columns[i];
                            switch (colCopyDef.getType()) {
                                case ColCopyDef.INTEGER_TYPE:
                                    int ival = resultSet.getInt(colCopyDef.getName());
                                    if (resultSet.wasNull()) {
                                        row[i] = null;
                                    } else {
                                        row[i] = "" + ival;
                                    }
                                    break;
                                case ColCopyDef.STRING_TYPE:
                                    String sval = resultSet.getString(colCopyDef.getName());
                                    if (sval != null) {
                                        if (sval.contains("\u2010")) {
                                            log.info("Invalid hyphen " + copyDef.getSrcTable() + ":" + colCopyDef.getName() + " '" + sval + "'");
                                            sval = sval.replaceAll("\u2010", "-");
                                        }
                                        if (colCopyDef.getMaxLen() > 0 && sval.length() > colCopyDef.getMaxLen()) {
                                            sval = sval.substring(0, colCopyDef.getMaxLen() - 1);
                                        }
                                    }
                                    row[i] = sval;
                                    break;
                                case ColCopyDef.DECIMAL_TYPE:
                                    BigDecimal dval = resultSet.getBigDecimal(colCopyDef.getName());
                                    if (dval == null || resultSet.wasNull()) {
                                        row[i] = null;
                                    } else {
                                        row[i] = dval.toPlainString();
                                    }
                                    break;
                                default:
                                    log.warn("ERROR: unknown column type for " + colCopyDef.getName());
                            }
                        }
                        bulkProcessor.add(row);
                        stats.rowsProcessed++;
                        stats.intervalRowsProcessed++;

                        if (stats.intervalRowsProcessed == 100000) {
                            stats.intervalRowsProcessed = 0;
                            long elapsed = System.currentTimeMillis() - stats.startTime;
                            log.info("Fetch Stats  " + copyDef.getSrcTable() + "," + stats.rowsProcessed +
                                    ", overall r/s: " + ((stats.rowsProcessed * 1000) / elapsed));
                        }
                    }
                }
                long elapsed = System.currentTimeMillis() - stats.startTime;
                log.info("READ FINISHED " + copyDef.getSrcTable() + "," + stats.rowsProcessed + ", r/s: " + ((stats.rowsProcessed * 1000) / elapsed));
                completedNoError = true;
            } catch (Throwable ex) {
                log.error(ex.getMessage(), ex);
            }
            if(completedNoError) {
                postProcessTable(copyDef, msps);
            }
        } catch (Throwable ex) {
            log.warn(ex.getMessage(), ex);
        }
    }

    public void copyTable(CopyDef copyDef, MapSqlParameterSource msps) {
        log.info("Copy table: " + copyDef.getSrcTable());
        try {
            serialBulkLoadTable(copyDef, msps);
        } catch (Throwable ex) {
            log.error(copyDef.getSrcTable() + ": " + ex.getMessage(),ex);
        }
    }

//    public void copyTables() {
//        for(CopyDef copyDef : copyDefs) {
//            copyTable(copyDef);
//        }
//    }
}