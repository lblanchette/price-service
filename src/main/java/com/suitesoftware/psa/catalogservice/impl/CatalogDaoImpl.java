package com.suitesoftware.psa.catalogservice.impl;

import com.suitesoftware.psa.catalogservice.dto.Catalog;
import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.dto.Part;
import com.suitesoftware.psa.catalogservice.dto.PartList;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
//import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

import jakarta.xml.bind.JAXB;
import java.io.OutputStream;
import java.math.BigDecimal;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.stream.Collectors;

/**
 * User: lrb
 * Date: 3/15/11
 * Time: 1:26 PM
 * (c) Copyright Suite Business Software
 */
public class CatalogDaoImpl implements com.suitesoftware.psa.catalogservice.CatalogDao {
    private static final Logger log = LogManager.getLogger(CatalogDaoImpl.class);

    NamedParameterJdbcTemplate jdbcTemplate;

    DataSource dataSource;

    String getCustomerCatalogSql;

    String updateCustomerItemPricesSql;
    String updateCustomerGroupPricesSql;
    String updateCustomerLevelPricesSql;
    String insertBasePricesSql;

    String queryBasePriceListSql;

    String updateCustomerLevelPriceMapSql;
    String updateCustomerGroupPriceMapSql;
    String updateCustomerItemPriceMapSql;
    String insertCustomerPriceSql;
    String updateCustomerPriceSql;
    String discontinueCustomerPriceSql;
    String catalogCustomersSql;

    String queryCachedCustomerPartsSql;


    String queryCustomerPriceListCacheQuerySql;

    //String updateCombinedCustomerPriceMapSql;

    String selectNsCustomersSql;

    String queryBasePartSql;

    String getCatalogCustomerSql;

    String getCacheCustomerPartsSql;

    String queryCachePartsListSql;

    String upsertCustomerPricesSql;

    String discontinueCustomerPricesSql;

    String upsertCacheItemsSql;

    String discontinueCacheItemsSql;

    String queryCacheCustomerItemPricesSql;

    String selectNsCatalogCustomersSql;

    String requestLogReportSql;

    public String getSelectNsCustomersSql() {
        return selectNsCustomersSql;
    }

    public void setSelectNsCustomersSql(String selectNsCustomersSql) {
        this.selectNsCustomersSql = selectNsCustomersSql;
    }

    public String getCatalogCustomersSql() {
        return catalogCustomersSql;
    }

    public void setCatalogCustomersSql(String catalogCustomersSql) {
        this.catalogCustomersSql = catalogCustomersSql;
    }

    public void setDataSource(DataSource dataSource) {
        jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.dataSource = dataSource;
    }

    public String getSelectNsCatalogCustomersSql() {
        return selectNsCatalogCustomersSql;
    }

    public void setSelectNsCatalogCustomersSql(String selectNsCatalogCustomersSql) {
        this.selectNsCatalogCustomersSql = selectNsCatalogCustomersSql;
    }

/*
    public String getUpdateCombinedCustomerPriceMapSql() {
        return updateCombinedCustomerPriceMapSql;
    }

    public void setUpdateCombinedCustomerPriceMapSql(String updateCombinedCustomerPriceMapSql) {
        this.updateCombinedCustomerPriceMapSql = updateCombinedCustomerPriceMapSql;
    }
*/
    public String getQueryBasePartSql() {
        return queryBasePartSql;
    }

    public void setQueryBasePartSql(String queryBasePartSql) {
        this.queryBasePartSql = queryBasePartSql;
    }

    public String getGetCatalogCustomerSql() {
        return getCatalogCustomerSql;
    }

    public void setGetCatalogCustomerSql(String getCatalogCustomerSql) {
        this.getCatalogCustomerSql = getCatalogCustomerSql;
    }

    public String getQueryCachedCustomerPartsSql() {
        return queryCachedCustomerPartsSql;
    }

    public void setQueryCachedCustomerPartsSql(String queryCachedCustomerPartsSql) {
        this.queryCachedCustomerPartsSql = queryCachedCustomerPartsSql;
    }

    public String getDiscontinueCustomerPriceSql() {
        return discontinueCustomerPriceSql;
    }

    public void setDiscontinueCustomerPriceSql(String discontinueCustomerPriceSql) {
        this.discontinueCustomerPriceSql = discontinueCustomerPriceSql;
    }

    public String getUpdateCustomerPriceSql() {
        return updateCustomerPriceSql;
    }

    public void setUpdateCustomerPriceSql(String updateCustomerPriceSql) {
        this.updateCustomerPriceSql = updateCustomerPriceSql;
    }

    public String getQueryCustomerPriceListCacheQuerySql() {
        return queryCustomerPriceListCacheQuerySql;
    }

    public void setQueryCustomerPriceListCacheQuerySql(String queryCustomerPriceListCacheQuerySql) {
        this.queryCustomerPriceListCacheQuerySql = queryCustomerPriceListCacheQuerySql;
    }

    public String getInsertCustomerPriceSql() {
        return insertCustomerPriceSql;
    }

    public void setInsertCustomerPriceSql(String insertCustomerPriceSql) {
        this.insertCustomerPriceSql = insertCustomerPriceSql;
    }

    public String getUpdateCustomerLevelPriceMapSql() {
        return updateCustomerLevelPriceMapSql;
    }

    public void setUpdateCustomerLevelPriceMapSql(String updateCustomerLevelPriceMapSql) {
        this.updateCustomerLevelPriceMapSql = updateCustomerLevelPriceMapSql;
    }

    public String getUpdateCustomerItemPriceMapSql() {
        return updateCustomerItemPriceMapSql;
    }

    public void setUpdateCustomerItemPriceMapSql(String updateCustomerItemPriceMapSql) {
        this.updateCustomerItemPriceMapSql = updateCustomerItemPriceMapSql;
    }

    public String getUpdateCustomerGroupPriceMapSql() {
        return updateCustomerGroupPriceMapSql;
    }

    public void setUpdateCustomerGroupPriceMapSql(String updateCustomerGroupPriceMapSql) {
        this.updateCustomerGroupPriceMapSql = updateCustomerGroupPriceMapSql;
    }

    public String getQueryBasePriceListSql() {
        return queryBasePriceListSql;
    }

    public void setQueryBasePriceListSql(String queryBasePriceListSql) {
        this.queryBasePriceListSql = queryBasePriceListSql;
    }

    public String getGetCustomerCatalogSql() {
        return getCustomerCatalogSql;
    }

    public String getUpdateCustomerItemPricesSql() {
        return updateCustomerItemPricesSql;
    }

    public void setUpdateCustomerItemPricesSql(String updateCustomerItemPricesSql) {
        this.updateCustomerItemPricesSql = updateCustomerItemPricesSql;
    }

    public String getUpdateCustomerGroupPricesSql() {
        return updateCustomerGroupPricesSql;
    }

    public void setUpdateCustomerGroupPricesSql(String updateCustomerGroupPricesSql) {
        this.updateCustomerGroupPricesSql = updateCustomerGroupPricesSql;
    }

    public String getUpdateCustomerLevelPricesSql() {
        return updateCustomerLevelPricesSql;
    }

    public void setUpdateCustomerLevelPricesSql(String updateCustomerLevelPricesSql) {
        this.updateCustomerLevelPricesSql = updateCustomerLevelPricesSql;
    }

    public String getDiscontinueCustomerPricesSql() {
        return discontinueCustomerPricesSql;
    }

    public void setDiscontinueCustomerPricesSql(String discontinueCustomerPricesSql) {
        this.discontinueCustomerPricesSql = discontinueCustomerPricesSql;
    }

    public String getUpsertCacheItemsSql() {
        return upsertCacheItemsSql;
    }

    public void setUpsertCacheItemsSql(String upsertCacheItemsSql) {
        this.upsertCacheItemsSql = upsertCacheItemsSql;
    }

    public String getDiscontinueCacheItemsSql() {
        return discontinueCacheItemsSql;
    }

    public void setDiscontinueCacheItemsSql(String discontinueCacheItemsSql) {
        this.discontinueCacheItemsSql = discontinueCacheItemsSql;
    }

    public String getInsertBasePricesSql() {
        return insertBasePricesSql;
    }

    public void setInsertBasePricesSql(String insertBasePricesSql) {
        this.insertBasePricesSql = insertBasePricesSql;
    }

    public void setGetCustomerCatalogSql(String getCustomerCatalogSql) {
        this.getCustomerCatalogSql = getCustomerCatalogSql;
    }

    public String getGetCacheCustomerPartsSql() {
        return getCacheCustomerPartsSql;
    }

    public void setGetCacheCustomerPartsSql(String getCacheCustomerPartsSql) {
        this.getCacheCustomerPartsSql = getCacheCustomerPartsSql;
    }

    public String getQueryCachePartsListSql() {
        return queryCachePartsListSql;
    }

    public void setQueryCachePartsListSql(String queryCachePartsListSql) {
        this.queryCachePartsListSql = queryCachePartsListSql;
    }

    public String getUpsertCustomerPricesSql() {
        return upsertCustomerPricesSql;
    }

    public void setUpsertCustomerPricesSql(String upsertCustomerPricesSql) {
        this.upsertCustomerPricesSql = upsertCustomerPricesSql;
    }

    public String getQueryCacheCustomerItemPricesSql() {
        return queryCacheCustomerItemPricesSql;
    }

    public void setQueryCacheCustomerItemPricesSql(String queryCacheCustomerItemPricesSql) {
        this.queryCacheCustomerItemPricesSql = queryCacheCustomerItemPricesSql;
    }

    public String getRequestLogReportSql() {
        return requestLogReportSql;
    }

    public void setRequestLogReportSql(String requestLogReportSql) {
        this.requestLogReportSql = requestLogReportSql;
    }

    String sqlParamSubst(Map<String,String> paramMap, String wrkSql) throws Exception {

         for(String key : paramMap.keySet()) {
             try {
                 wrkSql = wrkSql.replaceAll("\\{\\$ " + key + "\\}",paramMap.get(key));
             } catch (Throwable ex) {
                 log.error("sqlParamSubst:\nKEY: " + key + "\nVALUE:" + paramMap.get(key));
                 throw new Exception(ex.getMessage(),ex);
             }
         }
         return wrkSql;
     }

    @Override
    public void deleteCustomerPrices(int customerId) {
        String sql = "DELETE FROM CUSTOMER_PRICES WHERE CUSTOMER_ID = " + customerId;
        jdbcTemplate.update(sql,new MapSqlParameterSource());
    }


    @Override
    public void insertBasePrices(int customerId) {
        String sql = insertBasePricesSql;
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("customerId",""+customerId);
        jdbcTemplate.update(sql,paramMap);
    }


    @Override
    public void updateCustomerItemPrices(int customerId) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("customerId",customerId);
        jdbcTemplate.update(updateCustomerItemPricesSql,params);
    }

    @Override
    public void updateCustomerGroupPrices(int customerId) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("customerId", customerId);
        jdbcTemplate.update(updateCustomerGroupPricesSql, params);
    }

    @Override
    public void updateCustomerLevelPrices(Integer customerId) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("customerId", customerId);
        jdbcTemplate.update(updateCustomerLevelPricesSql, params);
    }

    @Override
    public Catalog getCustomerCatalog(int customerId, Calendar modifiedSince) {

        Catalog cat = new Catalog();
        cat.setCustomerId(customerId);
        cat.setPartList(new PartList());
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("customerId",customerId);
        List<Part> parts = jdbcTemplate.query(getGetCustomerCatalogSql(), params, new BeanPropertyRowMapper<Part>(Part.class));
        cat.getPartList().setPart(parts);
        return cat;
    }

    @Override
    public void updateCustomerPriceMap(final Map<Integer, BigDecimal> customerPriceMap, int customerId) {
        log.info("updateCustomerPriceMap: custId: " + customerId);
        long start = System.currentTimeMillis();

        jdbcTemplate.getJdbcOperations().query(updateCustomerLevelPriceMapSql,new Object[] {customerId},new int [] {Types.INTEGER}, new RowCallbackHandler() {
            public void processRow(ResultSet rs) throws SQLException {
                Integer itemId = rs.getInt("ITEM_ID");
                BigDecimal price = rs.getBigDecimal("PRICE");
                customerPriceMap.put(itemId,price);
            }
        });
        jdbcTemplate.getJdbcOperations().query(updateCustomerGroupPriceMapSql,new Object[] {customerId},new int [] {Types.INTEGER}, new RowCallbackHandler() {
            public void processRow(ResultSet rs) throws SQLException {
                Integer itemId = rs.getInt("ITEM_ID");
                BigDecimal price = rs.getBigDecimal("PRICE");
                customerPriceMap.put(itemId,price);
            }
        });
        jdbcTemplate.getJdbcOperations().query(updateCustomerItemPriceMapSql,new Object[] {customerId},new int [] {Types.INTEGER}, new RowCallbackHandler() {
            public void processRow(ResultSet rs) throws SQLException {
                Integer itemId = rs.getInt("ITEM_ID");
                BigDecimal price = rs.getBigDecimal("PRICE");
                customerPriceMap.put(itemId,price);
            }
        });
        log.info("updateCustomerPriceMap elapsed: " + (System.currentTimeMillis() - start));
    }

    @Override
    public void updateCustomerPriceMapNew(final Map<Integer, Part> basePartsMap, int customerId) {
        log.info("updateCustomerPriceMap: custId: " + customerId);
        long start = System.currentTimeMillis();
        String [] partPriceQuerySql = {
                updateCustomerLevelPriceMapSql,
                updateCustomerGroupPriceMapSql,
                updateCustomerItemPriceMapSql
        };

        for(String sql : partPriceQuerySql) {
            jdbcTemplate.getJdbcOperations().query(sql,new Object[] {customerId},new int [] {Types.INTEGER}, new RowCallbackHandler() {
                public void processRow(ResultSet rs) throws SQLException {
                    Integer itemId = rs.getInt("ITEM_ID");
                    BigDecimal price = rs.getBigDecimal("PRICE");
                    Part part = basePartsMap.get(itemId);
                    if(part != null) {
                        part.setCustomerPrice(price);
                    }
                }
            });
        }
        log.info("updateCustomerPriceMap elapsed: " + (System.currentTimeMillis() - start));
    }

    @Override
    public void streamCachedCustomerPartsList(final OutputStream outputStream, int customerId, Date lastModified) {

        String sql = getQueryCachedCustomerPartsSql();

        MapSqlParameterSource sps = new MapSqlParameterSource();
        sps.addValue("customerId",customerId);
        if (lastModified != null) {
            sps.addValue("lastModified",lastModified);
            sql += " AND last_modified > :lastModified";
        }
        final BeanPropertyRowMapper<Part> partMapper = new BeanPropertyRowMapper<Part>(Part.class);
        jdbcTemplate.getJdbcOperations().query(sql,new Object[] {customerId},new int [] {Types.INTEGER},new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet rs) throws SQLException {

                Part p = partMapper.mapRow(rs,1);
                JAXB.marshal(p,outputStream);
            }
        });
    }

    @Override
    public List<Part> getCachedCustomerPartsList(int customerId, Date modifiedSince) {

        String sql =  getQueryCachedCustomerPartsSql();

        MapSqlParameterSource sps = new MapSqlParameterSource();
        sps.addValue("customerId",customerId);
        if (modifiedSince != null) {
            sps.addValue("modifiedSince",modifiedSince);
            sql += " AND last_modified > :modifiedSince";
        }
        sql += " order by partNo asc";
        return jdbcTemplate.query(sql,sps,new BeanPropertyRowMapper<Part>(Part.class));
    }

    @Override
    public List<Part> getBasePriceList() throws Exception  {
        return jdbcTemplate.query(getQueryBasePriceListSql(),new BeanPropertyRowMapper<Part>(Part.class));
    }


    @Override
    public List<Integer> getPriceListCustomersIds() {
        return jdbcTemplate.query("select customer_id from customers where price_list_access_key is not null",new RowMapper<Integer>() {
            @Override
            public Integer mapRow(ResultSet rs, int i) throws SQLException {
                return rs.getInt("customer_id");
            }
        });
    }

    SqlParameterSource getPartSqlParams(int customerId, Part part, BigDecimal customerPrice) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("id",part.getId());
        msps.addValue("customerId",customerId);
        msps.addValue("price",customerPrice != null ? customerPrice : part.getPrice());
        msps.addValue("lastModified",Calendar.getInstance());
        msps.addValue("manPartNo",part.getManPartNo());
        msps.addValue("man",part.getMan());
        msps.addValue("desc",part.getDesc());
        msps.addValue("msrp",part.getMsrp());
        msps.addValue("discontinue",Boolean.FALSE);
        msps.addValue("vendor",part.getVendor());
        msps.addValue("partNo",part.getPartNo());
        return msps;
    }

    private void addParts(List<SqlParameterSource> spsList) {
        jdbcTemplate.batchUpdate(getInsertCustomerPriceSql(),spsList.toArray(new SqlParameterSource[0]));
    }

    private void updateParts(List<SqlParameterSource> spsList) {
        jdbcTemplate.batchUpdate(getUpdateCustomerPriceSql(),spsList.toArray(new SqlParameterSource[0]));
    }

    private void discontinueParts(List<SqlParameterSource> spsList) {
        jdbcTemplate.batchUpdate(getDiscontinueCustomerPriceSql(),spsList.toArray(new SqlParameterSource[0]));
    }

    /**
     * update customer price cache based on comparison with new calculated values from
     * base + customerPriceMap
     *
     * @param customerId customer
     * @param basePartsMap base parts
     * @param customerPriceMap customer prices
     */
    @Override
    public void updateCustomerCacheParts(int customerId, final Map<Integer, Part> basePartsMap, final Map<Integer, BigDecimal> customerPriceMap) {

        final List<Integer> changedIdList = new LinkedList<Integer>();
        final List<Integer> discontinuedIdList = new LinkedList<Integer>();
        final Map<Integer,Object> cachedPartIds = new HashMap<Integer, Object>(basePartsMap.size());

        log.info("updateCustomerCacheParts: start");
        /*
         * this is to be replaced
         */
        jdbcTemplate.getJdbcOperations().query(getQueryCustomerPriceListCacheQuerySql(),new Object[] {customerId},new int [] {Types.INTEGER},new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet rs) throws SQLException {

                Part cachePart = new Part();
                cachePart.setId(rs.getInt("id"));
                cachePart.setDiscontinue(rs.getBoolean("discontinue"));
                cachePart.setDesc(rs.getString("desc"));
                cachePart.setMan(rs.getString("man"));
                cachePart.setManPartNo(rs.getString("man_part_no"));
                cachePart.setMsrp(rs.getDouble("msrp"));
                cachePart.setPartNo(rs.getString("part_no"));
                cachePart.setPrice(rs.getBigDecimal("price"));
                cachePart.setVendor(rs.getString("vendor"));

                cachedPartIds.put(cachePart.getId(), null);

                Part updatePart = basePartsMap.get(cachePart.getId());
                if(updatePart == null) {
                    if(!cachePart.isDiscontinue()) {
                        discontinuedIdList.add(cachePart.getId());
                    }
                } else {
                    BigDecimal updateCustPrice = customerPriceMap.get(cachePart.getId());
                    if(updateCustPrice == null) {
                        updateCustPrice =  updatePart.getPrice();
                    }
                    if (updateCustPrice != null ? !updateCustPrice.equals(cachePart.getPrice()) : cachePart.getPrice() != null) {
                        changedIdList.add(updatePart.getId());
                    } else {
                        // already checked price, now check other stuff
                        cachePart.setPrice(updatePart.getPrice());
                        if(cachePart.changed(updatePart)) {
                            changedIdList.add(updatePart.getId());
                        }
                    }
                }
            }
        });
        List<SqlParameterSource> psList = new LinkedList<SqlParameterSource>();

        for(Integer partId : basePartsMap.keySet()) {
            if(!cachedPartIds.containsKey(partId)) {
                psList.add(getPartSqlParams(customerId, basePartsMap.get(partId),customerPriceMap.get(partId)));
            }
        }
        log.info("updateCustomerCacheParts: add parts " + psList.size());
        if(!psList.isEmpty()) {
            addParts(psList);
            psList.clear();
        }
        for(Integer partId : changedIdList) {
            psList.add(getPartSqlParams(customerId, basePartsMap.get(partId),customerPriceMap.get(partId)));
        }
        log.info("updateCustomerCacheParts: update parts " + psList.size());
        if(!psList.isEmpty()) {
            updateParts(psList);
            psList.clear();
        }
        for(Integer partId : discontinuedIdList) {
            MapSqlParameterSource msps = new MapSqlParameterSource();
            msps.addValue("id",partId);
            msps.addValue("customerId",customerId);
            psList.add(msps);
        }
        log.info("updateCustomerCacheParts: discontinue parts " + psList.size());
        if(!psList.isEmpty()) {
            discontinueParts(psList);
            psList.clear();
        }
        log.info("updateCustomerCacheParts: complete");
        return;
    }

    public List<CatalogCustomer> getKeyedCatalogCustomers(String accountId) {
            MapSqlParameterSource msps = new MapSqlParameterSource();
            msps.addValue("accountId",accountId);
            String sql = getCatalogCustomersSql() + " AND price_list_access_key is not null";
            return jdbcTemplate.query(sql,msps,new BeanPropertyRowMapper<>(CatalogCustomer.class));
    }

    @Override
    public List<CatalogCustomer> getCatalogCustomers(String accountId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("accountId",accountId);
        return jdbcTemplate.query(getCatalogCustomersSql(),msps,new BeanPropertyRowMapper<>(CatalogCustomer.class));
    }

    @Override
    public CatalogCustomer getCatalogCustomer(String accountId, int id) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("id",id);
        msps.addValue("accountId",accountId);
        String sql = getCatalogCustomersSql() + " AND id = :id";
        return jdbcTemplate.queryForObject(sql,msps, new BeanPropertyRowMapper<>(CatalogCustomer.class));
    }

/*
    @Override
    public List<Part> getCacheCustomerParts(String accountId, int customerId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("customerId",customerId);
        msps.addValue("accountId",accountId);

        return jdbcTemplate.query(getGetCacheCustomerPartsSql(),new BeanPropertyRowMapper<Part>(Part.class), msps);
    }
*/
    @Override
    public List<Part> getCachePartsList(String accountId, Boolean discontinue)  {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("accountId",accountId);
        StringBuilder sql = new StringBuilder(getQueryCachePartsListSql());
        if(discontinue != null) {
            sql.append(" AND discontinue = ").append(discontinue ? "true" : "false");
        }
        sql.append(" ORDER BY PART_NO");
        return jdbcTemplate.query(sql.toString(),msps,new BeanPropertyRowMapper<>(Part.class));
    }
    @Override
    public int upsertCacheCustomers(String accountId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("accountId",accountId);
        return dynamicUpsert("CUSTOMERS",msps);
        //return jdbcTemplate.update(getUpsertCacheItemsSql(),msps);
    }

    @Override
    public int upsertCacheItems(String accountId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("accountId",accountId);
        return dynamicUpsert("ITEMS",msps);
        //return jdbcTemplate.update(getUpsertCacheItemsSql(),msps);
    }
    @Override
    public int discontinueCacheItems(String accountId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("accountId",accountId);
        return jdbcTemplate.update(getDiscontinueCacheItemsSql(),msps);
    }
    @Override
    public int upsertCustomerPrices(String accountId, int customerId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("customerId",customerId);
        msps.addValue("accountId",accountId);
        return dynamicUpsert("CUSTOMER_ITEM_PRICES",msps);
        //return jdbcTemplate.update(getUpsertCustomerPricesSql(),msps);
    }
    @Override
    public int discontinueCustomerPrices(String accountId)   {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("accountId",accountId);
        return jdbcTemplate.update(getDiscontinueCustomerPricesSql(),msps);
    }

    @Override
    public void assignCacheCustomerPrices(Map<Integer, Part> basePartsMap, String accountId, int customerId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("customerId",customerId);
        msps.addValue("accountId",accountId);

        jdbcTemplate.query(getQueryCacheCustomerItemPricesSql(),msps,
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        int id = rs.getInt("id");
                        BigDecimal price = rs.getBigDecimal("price");
                        Date lastModified = rs.getDate("last_modified");
                        Part p = basePartsMap.get(id);
                        if(p != null) {
                            p.setCustomerPrice(price);
                            p.setCustomerPriceLastModified(lastModified);
                        }
                    }
                });
    }

    @Override
    public Part getCustomerPart(String accountId, int customerId, int partId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("partId",partId);
        msps.addValue("accountId",accountId);

        Part part = jdbcTemplate.queryForObject(getQueryBasePartSql(),msps,new BeanPropertyRowMapper<>(Part.class));

        String sql = getQueryCacheCustomerItemPricesSql() + " AND id = :partId";
        msps.addValue("customerId",customerId);
        jdbcTemplate.query(sql,msps,
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        BigDecimal price = rs.getBigDecimal("price");
                        Date lastModified = rs.getDate("last_modified");
                        part.setCustomerPrice(price);
                        part.setCustomerPriceLastModified(lastModified);
                    }
                });
        return part;
    }


    @Override
    public Part getBasePart(int partId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("partId",partId);
        return jdbcTemplate.queryForObject(getQueryBasePartSql(),msps,new BeanPropertyRowMapper<Part>(Part.class));
    }

    @Override
    public Integer getAccessCount(int customerId) {
        String getCatalogAccessCountSql =
                "SELECT count(*) FROM REQUEST_LOG WHERE CUSTOMER_ID = :customerId AND CREATE_TS > (CURRENT_TIMESTAMP - interval '24 hour') and STATUS = 'ACCEPTED'";
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("customerId",customerId);
        return jdbcTemplate.queryForObject(getCatalogAccessCountSql, msps, Integer.class);
    }
    /*
    INSERT INTO REQUEST_LOG (
        "CUSTOMER_ID", "STATUS", "CREATE_TS")
        VALUES (1, 1, CURRENT_TIMESTAMP);

     */
    @Override
    public int insertRequestLog(int customerId, String status, String response, int bytes) {
        String insertRequestLogSql =
                "INSERT INTO REQUEST_LOG (CUSTOMER_ID, STATUS, CREATE_TS) VALUES (:customerId, :status, CURRENT_TIMESTAMP)";
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("customerId",customerId);
        msps.addValue("status",status);
        msps.addValue("response",response);
        msps.addValue("bytes",bytes);
        return jdbcTemplate.update(insertRequestLogSql, msps);
    }

    @Override
    public List<Map<String, Object>> requestLogReport(Integer customerId, Date asOfDate) {

        MapSqlParameterSource msps = new MapSqlParameterSource();
        StringBuilder rlrSql = new StringBuilder(getRequestLogReportSql());
        if(customerId != null) {
            rlrSql.append(" AND (c.customer_id = :customerId)");
            msps.addValue("customerId",customerId);
        }
        if(asOfDate != null) {
//            DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
//            String sinceStr = asOfDate.format(formatter);
            rlrSql.append(" AND (rl.create_ts >= :sinceStr)");
            msps.addValue("sinceStr",asOfDate);
        }
        rlrSql.append(" order by request_date desc, request_time desc");
        return jdbcTemplate.queryForList(rlrSql.toString(),msps);
    }


    public int dynamicUpsert(String duName, Map<String,Object> params) {
        return dynamicUpsert(duName,new MapSqlParameterSource(params));
    }

    private String sqlEscape(String colName) {
        return "\"" + colName + "\"";
    }

    public int dynamicUpsert(String duName, MapSqlParameterSource msps) {

        class DynamicUpsertDef {
            DynamicUpsertDef(String name, String fromTable, String toTable, String excludeKey)  {
                this.name = name;
                this.fromTable = fromTable;
                this.toTable = toTable;
                this.excludeKey = excludeKey;
                //this.requiredParams = null;// = Arrays.asList(requiredParams);
            }
            //public List<String> requiredParams;
            public String name;
            public String fromTable;
            public String toTable;
            public String excludeKey;
        }

        Map<String,DynamicUpsertDef> dynamicUpsertDefs = new HashMap<>();
        dynamicUpsertDefs.put("ITEMS",
                new DynamicUpsertDef(
                        "ITEMS",
                        "xfer_items",
                        "cache_items",
                        "id"
                        //,new String[] {"accountId"}
                ));
        dynamicUpsertDefs.put("CUSTOMER_ITEM_PRICES",
                new DynamicUpsertDef(
                        "CUSTOMER_ITEM_PRICES",
                        "xfer_customer_item_prices",
                        "cache_customer_item_prices",
                        "id"
                        //,new String[] {"accountId","customerId"}
                ));
        dynamicUpsertDefs.put("CUSTOMERS",
                new DynamicUpsertDef(
                        "CUSTOMERS",
                        "xfer_customers",
                        "cache_customers",
                        "id"
                        //,new String[]{"accountId"}
                ));
        DynamicUpsertDef duDef = dynamicUpsertDefs.get(duName);

        if(duDef == null) {
            throw new IllegalArgumentException("DynamicUpsertDef not found for " + duName);
        }
//        duDef.requiredParams.forEach(param -> {
//            if(!msps.hasValue(param)) {
//                throw new IllegalArgumentException("Missing required parameter: " + param);
//            }
//        });
        List<String> columns = new LinkedList<>();
        List<String> primaryKeys = new LinkedList<>();
        List<String> excludeColumns  = new LinkedList<String>() {{
            add(sqlEscape("last_modified"));
            add(sqlEscape("discontinue"));
        }};
        // get metadata
        try(Connection conn = dataSource.getConnection()) {
            ResultSet rs = conn.getMetaData().getColumns(null,null,duDef.toTable,null);
            while(rs.next()) {
                columns.add(sqlEscape(rs.getString("COLUMN_NAME")));
            }
            rs.close();
            rs = conn.getMetaData().getPrimaryKeys(null,null,duDef.toTable);
            while(rs.next()) {
                primaryKeys.add(sqlEscape(rs.getString("COLUMN_NAME")));
            }
            rs.close();
        } catch (SQLException ex) {
            log.error("dynamicUpsert MetaData error: " + ex.getMessage(),ex);
        }

        Map<String,String> valuesMap = new HashMap<>();
        valuesMap.put("fromTable",duDef.fromTable);
        valuesMap.put("toTable",duDef.toTable);
        valuesMap.put("primaryKeys",String.join(", ", primaryKeys));

        valuesMap.put("columns", String.join(", ", columns));

        String compareColumns = columns.stream().filter(column -> !excludeColumns.contains(column)).collect(Collectors.joining(", "));

        valuesMap.put("compareFromColumns",compareColumns + (columns.contains(sqlEscape("discontinue")) ? ", false as discontinue" : ""));
        valuesMap.put("compareToColumns",compareColumns + (columns.contains(sqlEscape("discontinue")) ? ", discontinue" : ""));

        valuesMap.put("compareWhere", primaryKeys.stream().map(column -> {
            if(sqlEscape(duDef.excludeKey).equalsIgnoreCase(column))
                return "(1=1)";
            if(sqlEscape("account_id").equalsIgnoreCase(column) )
                return column + "  = :accountId";
            if(sqlEscape("customer_id").equalsIgnoreCase(column) )
                return column + " = :customerId";
            throw new IllegalArgumentException("Primary key column not supported: " + column);
        }).collect(Collectors.joining(" AND ")));

        valuesMap.put("changedColumns",columns.stream().map(column -> sqlEscape("last_modified").equalsIgnoreCase(column) ? "NOW()" : "CHANGED." + column).collect(Collectors.joining(", ")));

        valuesMap.put("excludedKeys", String.join(", ", primaryKeys));

        valuesMap.put("excludedColumns",columns.stream().filter(column -> !primaryKeys.contains(column)).map(column ->
                column + " = " + (sqlEscape("last_modified").equalsIgnoreCase(column) ? "NOW()" : "EXCLUDED." + column)
        ).collect(Collectors.joining(", ")));


        String upsertTemplate =
            "WITH CHANGED AS (" +
            "SELECT ${compareFromColumns} FROM ${fromTable} WHERE ${compareWhere} " +
            " EXCEPT " +
            "SELECT ${compareToColumns} FROM ${toTable} WHERE ${compareWhere} ) " +
            " INSERT INTO ${toTable} ( ${columns} ) " +
            " SELECT ${changedColumns} " +
            " FROM CHANGED " +
            " ON CONFLICT (${primaryKeys}) DO UPDATE SET " +
            " ${excludedColumns}";

        StringSubstitutor stringSubstitutor = new StringSubstitutor(valuesMap);
        String sql = stringSubstitutor.replace(upsertTemplate);
        log.info("dynamicUpsert: " + sql);
        return jdbcTemplate.update(sql,msps);

//        StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
//        StringBuilder whereSql = new StringBuilder(" WHERE ");
//        MapSqlParameterSource msps = new MapSqlParameterSource();
//        for(String key : values.keySet()) {
//            sql.append(key).append(" = :").append(key).append(",");
//            msps.addValue(key,values.get(key));
//        }
//        sql.deleteCharAt(sql.length()-1);
//        for(String key : where.keySet()) {
//            whereSql.append(key).append(" = :").append(key).append(" AND ");
//            msps.addValue(key,where.get(key));
//        }
//        whereSql.delete(whereSql.length()-5,whereSql.length());
//        sql.append(whereSql);
//        return jdbcTemplate.update(sql.toString(),msps);
    }
}