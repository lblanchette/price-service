package com.suitesoftware.psa.catalogservice.impl;

import com.suitesoftware.psa.catalogservice.NsDataSourceFactory;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * This needs enhancement/refactoring to work for multi NS accounts
 */

public class NsDataSourceFactoryImpl implements NsDataSourceFactory {

    private final Logger log = Logger.getLogger(getClass());

    private final String connectionStringFormat = "jdbc:ns://%s.%s:%d;ServerDataSource=%s;encrypted=1;CustomProperties=(AccountID=%s;RoleID=%d)";
    private Driver nsDriver; //com.netsuite.jdbc.openaccess.OpenAccessDriver
    private String nsJdbcHost;
    private int nsJdbcPort;
    private String nsJdbcDataSource;
    private String nsJdbcAccountId;
    private String nsJdbcDriverClass;

    private String nsUrlTemplate;

    private String nsJdbcRoleId;

    private String nsAdminName;
    private String nsAdminPassword;
    private String nsAdminRole;


    public Driver getNsDriver() {
        return nsDriver;
    }

    public void setNsDriver(Driver nsDriver) {
        this.nsDriver = nsDriver;
    }

    public String getNsAdminName() {
        return nsAdminName;
    }

    public void setNsAdminName(String nsAdminName) {
        this.nsAdminName = nsAdminName;
    }

    public String getNsAdminPassword() {
        return nsAdminPassword;
    }

    public void setNsAdminPassword(String nsAdminPassword) {
        this.nsAdminPassword = nsAdminPassword;
    }

    public String getNsAdminRole() {
        return nsAdminRole;
    }

    public void setNsAdminRole(String nsAdminRole) {
        this.nsAdminRole = nsAdminRole;
    }

    public String getNsJdbcHost() {
        return nsJdbcHost;
    }

    public void setNsJdbcHost(String nsJdbcHost) {
        this.nsJdbcHost = nsJdbcHost;
    }

    public int getNsJdbcPort() {
        return nsJdbcPort;
    }

    public void setNsJdbcPort(int nsJdbcPort) {
        this.nsJdbcPort = nsJdbcPort;
    }

    public String getNsJdbcDataSource() { return nsJdbcDataSource;}

    public void setNsJdbcDataSource(String nsJdbcDataSource) { this.nsJdbcDataSource = nsJdbcDataSource;}

    public String getNsJdbcAccountId() {
        return nsJdbcAccountId;
    }

    public void setNsJdbcAccountId(String nsJdbcAccountId) {
        this.nsJdbcAccountId = nsJdbcAccountId;
    }

    public String getNsJdbcDriverClass() {
        return nsJdbcDriverClass;
    }

    public void setNsJdbcDriverClass(String nsJdbcDriverClass) {
        this.nsJdbcDriverClass = nsJdbcDriverClass;
    }

    public String getNsUrlTemplate() {
        return nsUrlTemplate;
    }

    public void setNsUrlTemplate(String nsUrlTemplate) {
        this.nsUrlTemplate = nsUrlTemplate;
    }

    public String getNsJdbcRoleId() {
        return nsJdbcRoleId;
    }

    public void setNsJdbcRoleId(String nsJdbcRoleId) {
        this.nsJdbcRoleId = nsJdbcRoleId;
    }

    private Map<String, String> adminDbParams() {
        Map<String, String> params = new HashMap<>();
        params.put("ns_username", getNsAdminName());
        params.put("ns_password", getNsAdminPassword());
        params.put("ns_role_id", getNsAdminRole());
        return params;
    }

    private void testConnection(DataSource ds) throws SQLException {
        log.debug("Test connection");
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().executeQuery("select 1").next();
            log.info("NS DB Test Connection OK");
        } catch (SQLException ex) {
            log.info("NS DB Test Connection FAILED", ex);
            throw ex;
        }
    }

    public DataSource newAdminPooledNsDataSource(int poolSize) throws SQLException {
        return newPooledNsDataSource(poolSize, adminDbParams());
    }

    public DataSource newPooledNsDataSource(int poolSize, Map<String,String> params) throws SQLException {

        String nsUsername = params.get("ns_username");
        String nsPassword = params.get("ns_password");
        int nsRoleId = Integer.parseInt(params.get("ns_role_id"));

        log.info("newPooledNsDataSource Acct: " + nsJdbcAccountId + " User: " +  nsUsername + " Role: " + nsRoleId);

        BasicDataSource dataSource = new BasicDataSource();

        dataSource.setDriver(nsDriver);
        dataSource.setUrl(String.format(connectionStringFormat,nsJdbcAccountId, nsJdbcHost, nsJdbcPort, nsJdbcDataSource,  nsJdbcAccountId, nsRoleId));
        dataSource.setUsername(nsUsername);
        dataSource.setPassword(nsPassword);
        dataSource.setMaxTotal(poolSize);
        dataSource.setMaxIdle(poolSize);
        dataSource.setTimeBetweenEvictionRunsMillis(20000);
        dataSource.setMaxOpenPreparedStatements(10);
        dataSource.setTestOnBorrow(true);

        testConnection(dataSource);
        return dataSource;
    }

    public DataSource newAdminNsDataSource() throws SQLException {
        return newNsDataSource(adminDbParams());
    }

    public DataSource newNsDataSource(Map<String,String> params) throws SQLException {

        String nsUsername = params.get("ns_username");
        String nsPassword = params.get("ns_password");
        int nsRoleId = Integer.parseInt(params.get("ns_role_id"));

        log.info("newNsDataSource Acct: " + nsJdbcAccountId + " User: " +  nsUsername + " Role: " + nsRoleId);

        String connectionString = String.format(connectionStringFormat, nsJdbcAccountId, nsJdbcHost, nsJdbcPort, nsJdbcDataSource,  nsJdbcAccountId, nsRoleId);
        log.debug("Setup Ns DataSource connectionString: '" + connectionString + "'");
        SimpleDriverDataSource nsDataSource = new SimpleDriverDataSource(nsDriver,connectionString,nsUsername,nsPassword);
        testConnection(nsDataSource);
        return nsDataSource;
    }
}
