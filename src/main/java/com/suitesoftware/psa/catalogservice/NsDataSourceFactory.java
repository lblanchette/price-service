package com.suitesoftware.psa.catalogservice;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;

public interface NsDataSourceFactory {

    DataSource newAdminNsDataSource() throws SQLException;

    DataSource newNsDataSource(Map<String, String> params) throws SQLException;

    DataSource newAdminPooledNsDataSource(int poolSize) throws SQLException;

    DataSource newPooledNsDataSource(int poolSize, Map<String, String> params) throws SQLException;

    String getNsJdbcAccountId();

    void setNsJdbcAccountId(String nsJdbcAccountId);
}