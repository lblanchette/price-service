package com.suitesoftware.psa.catalogservice.impl;

import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.dto.Part;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

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

    String selectNsCustomersSql;
    String selectNsBasePartsSql;
    String selectNsCustomerPricesSql;
//    String selectAllNsCustomersSql;

    String selectCatalogCustomersSql;

    public String getSelectCatalogCustomersSql() {
        return selectCatalogCustomersSql;
    }

    public void setSelectCatalogCustomersSql(String selectCatalogCustomersSql) {
        this.selectCatalogCustomersSql = selectCatalogCustomersSql;
    }

    public String getSelectNsCustomersSql() {
        return selectNsCustomersSql;
    }

    public void setSelectNsCustomersSql(String selectNsCustomersSql) {
        this.selectNsCustomersSql = selectNsCustomersSql;
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

    public void setDatasource(DataSource datasource) {
        this.datasourceTL.set(datasource);
    }

    @Override
    public CatalogCustomer getCatalogCustomer(Integer customerId) {
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(datasourceTL.get());
        //SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(datasourceTL.get());
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("customerId",customerId);
        String sql = getSelectCatalogCustomersSql() + " WHERE customer_id = :customerId and account_id = :accountId";
        //return jdbcTemplate.queryForObject(sql,new BeanPropertyRowMapper<CatalogCustomer>(CatalogCustomer.class), msps);
        return jdbcTemplate.queryForObject(sql,msps,new BeanPropertyRowMapper<CatalogCustomer>(CatalogCustomer.class));
    }


    @Override
    public List<CatalogCustomer> getCatalogCustomers() {
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(datasourceTL.get());
        //SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(datasourceTL.get());
        MapSqlParameterSource msps = new MapSqlParameterSource();
        //return jdbcTemplate.query(getSelectCatalogCustomersSql(),new BeanPropertyRowMapper<CatalogCustomer>(CatalogCustomer.class),new HashMap<>());
        return jdbcTemplate.query(getSelectCatalogCustomersSql(),new BeanPropertyRowMapper<>(CatalogCustomer.class));
    }

    @Override
    public List<Part> getBaseParts() {
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(datasourceTL.get());
        //SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(datasourceTL.get());
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("priceId",priceId);
        msps.addValue("quantityId",quantityId);
        msps.addValue("currencyId",currencyId);
//        return jdbcTemplate.getNamedParameterJdbcOperations().query(getSelectNsBasePartsSql(),msps,
//                new BeanPropertyRowMapper<Part>(Part.class));
        return jdbcTemplate.queryForList(getSelectNsBasePartsSql(),msps,Part.class);
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
        //SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(datasourceTL.get());
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(datasourceTL.get());

        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("priceId",priceId);
        msps.addValue("quantityId",quantityId);
        msps.addValue("currencyId",currencyId);
        msps.addValue("customerId",customerId);

        //jdbcTemplate.getNamedParameterJdbcOperations().query(getSelectNsCustomerPricesSql(), msps,
        jdbcTemplate.query(getSelectNsCustomerPricesSql(), msps,
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
