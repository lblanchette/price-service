package com.suitesoftware.psa.catalogservice.impl;

import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.dto.Part;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NsDaoImpl implements com.suitesoftware.psa.catalogservice.NsDao {

    Integer priceId = 5;
    Integer quantityId = 1;
    Integer currencyId = 1;

    private ThreadLocal<DataSource> datasourceTL = new ThreadLocal<>();

    String selectNsCatalogCustomerSql;

    String selectNsCatalogCustomersSql;
    String selectNsBasePartsSql;
    String selectNsCustomerPricesSql;
//    String selectAllNsCustomersSql;

    public String getSelectNsCatalogCustomerSql() {
        return selectNsCatalogCustomerSql;
    }

    public void setSelectNsCatalogCustomerSql(String selectNsCatalogCustomerSql) {
        this.selectNsCatalogCustomerSql = selectNsCatalogCustomerSql;
    }

    public String getSelectNsCatalogCustomersSql() {
        return selectNsCatalogCustomersSql;
    }

    public void setSelectNsCatalogCustomersSql(String selectNsCatalogCustomersSql) {
        this.selectNsCatalogCustomersSql = selectNsCatalogCustomersSql;
    }

    public String getSelectNsBasePartsSql() {
        return selectNsBasePartsSql;
    }

    public void setSelectNsBasePartsSql(String selectNsBasePartsSql) {
        this.selectNsBasePartsSql = selectNsBasePartsSql;
    }

    public String getSelectNsCustomerPricesSql() {
        return selectNsCustomerPricesSql;
    }

    public void setSelectNsCustomerPricesSql(String selectNsCustomerPricesSql) {
        this.selectNsCustomerPricesSql = selectNsCustomerPricesSql;
    }

//    public String getSelectAllNsCustomersSql() {
//        return selectAllNsCustomersSql;
//    }
//
//    public void setSelectAllCustomersSql(String selectAllNsCustomersSql) {
//        this.selectAllNsCustomersSql = selectAllNsCustomersSql;
//    }

    public void setDatasource(DataSource datasource) {
        this.datasourceTL.set(datasource);
    }

    @Override
    public CatalogCustomer getCatalogCustomer(Integer customerId) {
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(datasourceTL.get());
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("customerId",customerId);
        return jdbcTemplate.queryForObject(getSelectNsCatalogCustomerSql(),new BeanPropertyRowMapper<CatalogCustomer>(CatalogCustomer.class), msps);
    }


    @Override
    public List<CatalogCustomer> getCatalogCustomers() {
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(datasourceTL.get());
        MapSqlParameterSource msps = new MapSqlParameterSource();
        return jdbcTemplate.query(getSelectNsCatalogCustomersSql(),new BeanPropertyRowMapper<CatalogCustomer>(CatalogCustomer.class),new HashMap<>());
    }

    @Override
    public List<Part> getBaseParts() {
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(datasourceTL.get());
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("priceId",priceId);
        msps.addValue("quantityId",quantityId);
        msps.addValue("currencyId",currencyId);
        return jdbcTemplate.getNamedParameterJdbcOperations().query(getSelectNsBasePartsSql(),msps,
                new BeanPropertyRowMapper<Part>(Part.class));
    }

/*
    @Override
    public List<CatalogCustomer> getAllCustomers() {
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(datasourceTL.get());
        MapSqlParameterSource msps = new MapSqlParameterSource();
        return jdbcTemplate.query(getSelectAllNsCustomersSql(),new BeanPropertyRowMapper<CatalogCustomer>(CatalogCustomer.class),new HashMap<>());
    }
*/
    @Override
    public void assignCustomerPrices(Map<Integer, Part> partMap, int customerId) {
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(datasourceTL.get());
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("priceId",priceId);
        msps.addValue("quantityId",quantityId);
        msps.addValue("currencyId",currencyId);
        msps.addValue("customerId",customerId);

        jdbcTemplate.getNamedParameterJdbcOperations().query(getSelectNsCustomerPricesSql(), msps,
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        int id = rs.getInt("id");
                        BigDecimal price = rs.getBigDecimal("price");
                        if(price != null) {
                            Part part = partMap.get(id);
                            if(part != null) {
                                part.setCustomerPrice(price);
                            }
                        }
                    }
                });
    }
}
