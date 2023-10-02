package com.suitesoftware.psa.catalogservice.impl;

import com.suitesoftware.psa.catalogservice.dto.Catalog;
import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.dto.Part;
import com.suitesoftware.psa.catalogservice.dto.PartList;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

import javax.sql.DataSource;
import javax.xml.bind.JAXB;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

/**
 * User: lrb
 * Date: 3/15/11
 * Time: 1:26 PM
 * (c) Copyright Suite Business Software
 */
public class CatalogDaoImpl implements com.suitesoftware.psa.catalogservice.CatalogDao {

    private Logger log = Logger.getLogger(getClass());

//    @Autowired
//    private PlatformTransactionManager platformTransactionManager;

    SimpleJdbcTemplate jdbcTemplate;

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

    String queryCachedCustomerPartsSql;


    String queryCustomerPriceListCacheQuerySql;

    String updateCombinedCustomerPriceMapSql;

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

    public void setDataSource(DataSource dataSource) {
        jdbcTemplate = new SimpleJdbcTemplate(dataSource);
        this.dataSource = dataSource;
    }

    public String getSelectNsCatalogCustomersSql() {
        return selectNsCatalogCustomersSql;
    }

    public void setSelectNsCatalogCustomersSql(String selectNsCatalogCustomersSql) {
        this.selectNsCatalogCustomersSql = selectNsCatalogCustomersSql;
    }

    public String getUpdateCombinedCustomerPriceMapSql() {
        return updateCombinedCustomerPriceMapSql;
    }

    public void setUpdateCombinedCustomerPriceMapSql(String updateCombinedCustomerPriceMapSql) {
        this.updateCombinedCustomerPriceMapSql = updateCombinedCustomerPriceMapSql;
    }

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
        jdbcTemplate.update(sql);
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
        List<Part> parts = jdbcTemplate.query(getGetCustomerCatalogSql(),new BeanPropertyRowMapper<Part>(Part.class), params);
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

        String sql =  queryCachedCustomerPartsSql;

        MapSqlParameterSource sps = new MapSqlParameterSource();
        sps.addValue("customerId",customerId);
        if (lastModified != null) {
            sps.addValue("lastModified",lastModified);
            sql += " AND last_modified > :lastModified";
        }
        final BeanPropertyRowMapper<Part> partMapper = new BeanPropertyRowMapper<Part>(Part.class);
        jdbcTemplate.getJdbcOperations().query(queryCachedCustomerPartsSql,new Object[] {customerId},new int [] {Types.INTEGER},new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet rs) throws SQLException {

                Part p = partMapper.mapRow(rs,1);
                JAXB.marshal(p,outputStream);
/*
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
 */
            }
        });
    }

    @Override
    public List<Part> getCachedCustomerPartsList(int customerId, Date modifiedSince) {

        String sql =  queryCachedCustomerPartsSql;

        MapSqlParameterSource sps = new MapSqlParameterSource();
        sps.addValue("customerId",customerId);
        if (modifiedSince != null) {
            sps.addValue("modifiedSince",modifiedSince);
            sql += " AND last_modified > :modifiedSince";
        }
        sql += " order by partNo asc";
        return jdbcTemplate.query(sql,new BeanPropertyRowMapper<Part>(Part.class),sps);
    }
/*
    public List<Part> getCachedCustomerPart(int customerId, int partId) {

        String sql =  queryCachedCustomerPartsSql;

        MapSqlParameterSource sps = new MapSqlParameterSource();
        sps.addValue("customerId",customerId);
        sps.addValue("id",partId);
        sql += " AND id = :id";
        return jdbcTemplate.query(sql,new BeanPropertyRowMapper<Part>(Part.class),sps);
    }
*/

    @Override
    public List<Part> getBasePriceList() throws Exception  {
        return jdbcTemplate.query(queryBasePriceListSql,new BeanPropertyRowMapper<Part>(Part.class));

//        String sql = queryBasePriceListSql;
//        Map<String,String> paramMap = new HashMap<String, String>();
//        paramMap.put("CUSTOMER_ID",""+customerId);
//        sql = sqlParamSubst(paramMap,sql);
//        jdbcTemplate.update(sql,paramMap);
    }


    @Override
    public List<Integer> getPriceListCustomersIds() {
        return jdbcTemplate.query("select customer_id from customers where price_list_access_key is not null",new RowMapper<Integer>() {
            @Override
            public Integer mapRow(ResultSet rs, int i) throws SQLException {
                return rs.getInt("customer_id");
            }
        });
//        String sql = queryBasePriceListSql;
//        Map<String,String> paramMap = new HashMap<String, String>();
//        paramMap.put("CUSTOMER_ID",""+customerId);
//        sql = sqlParamSubst(paramMap,sql);
//        jdbcTemplate.update(sql,paramMap);
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
        jdbcTemplate.batchUpdate(insertCustomerPriceSql,spsList.toArray(new SqlParameterSource[spsList.size()]));
    }

    private void addPart(int customerId, Part part, BigDecimal customerPrice) {
        SqlParameterSource sps = getPartSqlParams(customerId, part,customerPrice);
        jdbcTemplate.update(insertCustomerPriceSql,sps);
    }

    private void updateParts(List<SqlParameterSource> spsList) {
        jdbcTemplate.batchUpdate(updateCustomerPriceSql,spsList.toArray(new SqlParameterSource[spsList.size()]));
    }

    private void updatePart(int customerId, Part part, BigDecimal customerPrice) {
        SqlParameterSource sps = getPartSqlParams(customerId, part,customerPrice);
        jdbcTemplate.update(updateCustomerPriceSql,sps);
    }

    private void discontinueParts(List<SqlParameterSource> spsList) {
        jdbcTemplate.batchUpdate(discontinueCustomerPriceSql,spsList.toArray(new SqlParameterSource[spsList.size()]));
    }

    private void discontinuePart(int customerId, int partId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("id",partId);
        msps.addValue("customerId",customerId);
        jdbcTemplate.update(discontinueCustomerPriceSql,msps);
    }

    /**
     *
     */
/*
    public void combinedCacheParts(int customerId) {

        jdbcTemplate.getJdbcOperations().query(updateCombinedCustomerPriceMapSql,
                new Object[] {customerId,customerId,customerId,customerId},
                new int [] {Types.INTEGER,Types.INTEGER,Types.INTEGER,Types.INTEGER,},
                new RowCallbackHandler() {
                    public void processRow(ResultSet rs) throws SQLException {
                        int id = rs.getInt("ITEM_ID");
                        BigDecimal price = rs.getBigDecimal("price");
                        System.out.println();
                    }
        });

    }

*/
    /**
     * update customer price cache based on comparison with new calculated values from
     * base + customerPriceMap
     *
     * @param customerId
     * @param basePartsMap
     * @param customerPriceMap
     */
    @Override
    public void updateCustomerCacheParts(int customerId, final Map<Integer, Part> basePartsMap, final Map<Integer, BigDecimal> customerPriceMap) {

        final List<Integer> changedIdList = new LinkedList<Integer>();
        final List<Integer> discontinuedIdList = new LinkedList<Integer>();
        final Map<Integer,Object> cachedPartIds = new HashMap<Integer, Object>(basePartsMap.size());

        final BeanPropertyRowMapper<Part> partMapper = new BeanPropertyRowMapper<Part>(Part.class);

        log.info("updateCustomerCacheParts: start");

        /**
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
//        if(true) {
//            throw new Error("Test error");
//        }

        List<SqlParameterSource> psList = new LinkedList<SqlParameterSource>();

        for(Integer partId : basePartsMap.keySet()) {
            if(!cachedPartIds.containsKey(partId)) {
                psList.add(getPartSqlParams(customerId, basePartsMap.get(partId),customerPriceMap.get(partId)));
            }
        }
        log.info("updateCustomerCacheParts: add parts " + psList.size());
        if(psList.size() > 0) {
            addParts(psList);
            psList.clear();
        }
        for(Integer partId : changedIdList) {
            psList.add(getPartSqlParams(customerId, basePartsMap.get(partId),customerPriceMap.get(partId)));
            //updatePart(customerId, basePartsMap.get(partId), customerPriceMap.get(partId));
        }
        log.info("updateCustomerCacheParts: update parts " + psList.size());
        if(psList.size() > 0) {
            updateParts(psList);
            psList.clear();
        }
        for(Integer partId : discontinuedIdList) {
            MapSqlParameterSource msps = new MapSqlParameterSource();
            msps.addValue("id",partId);
            msps.addValue("customerId",customerId);
            psList.add(msps);
//            psList.add(getPartSqlParams(customerId, basePartsMap.get(partId),customerPriceMap.get(partId)));
//            discontinuePart(customerId,partId);
        }
        log.info("updateCustomerCacheParts: discontinue parts " + psList.size());
        if(psList.size() > 0) {
            discontinueParts(psList);
            psList.clear();
        }
        log.info("updateCustomerCacheParts: complete");
        return;
    }


    @Override
    public List<CatalogCustomer> getCatalogCustomers() {
        return jdbcTemplate.query(getSelectNsCatalogCustomersSql(),new BeanPropertyRowMapper<CatalogCustomer>(CatalogCustomer.class),new HashMap<>());
    }

    @Override
    public CatalogCustomer getCustomer(int customerId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("customerId",customerId);
        return jdbcTemplate.queryForObject(getCatalogCustomerSql,new BeanPropertyRowMapper<CatalogCustomer>(CatalogCustomer.class), msps);
    }

    @Override
    public List<Part> getCacheCustomerParts(int customerId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("customerId",customerId);
        List<Part> parts = jdbcTemplate.query(getGetCacheCustomerPartsSql(),new BeanPropertyRowMapper<Part>(Part.class), msps);
        return parts;
    }

    @Override
    public List<Part> getCachePartsList(Boolean discontinue)  {
        StringBuilder sql = new StringBuilder(getQueryCachePartsListSql()).append(" WHERE (1=1)");
        if(discontinue != null) {
            sql.append(" AND discontinue = ").append(discontinue ? "true" : "false");
        }
        sql.append(" ORDER BY PART_NO");
        return jdbcTemplate.query(sql.toString(),new BeanPropertyRowMapper<Part>(Part.class));
    }

    @Override
    public int upsertCacheItems() {
        return jdbcTemplate.update(getUpsertCacheItemsSql());
    }
    @Override
    public int discontinueCacheItems() {
        return jdbcTemplate.update(getDiscontinueCacheItemsSql());
    }
    @Override
    public int upsertCustomerPrices(int customerId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("customerId",customerId);
        return jdbcTemplate.update(getUpsertCustomerPricesSql(),msps);
    }
    @Override
    public int discontinueCustomerPrices() {
        return jdbcTemplate.update(getDiscontinueCustomerPricesSql());
    }

    @Override
    public void assignCacheCustomerPrices(Map<Integer, Part> basePartsMap, int customerId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("customerId",customerId);
        jdbcTemplate.getNamedParameterJdbcOperations().query(queryCacheCustomerItemPricesSql,msps,
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
    public Part getCustomerPart(int customerId, int partId) {
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("partId",partId);
        Part part = jdbcTemplate.queryForObject(queryBasePartSql,new BeanPropertyRowMapper<Part>(Part.class),msps);

        StringBuilder sql = new StringBuilder(queryCacheCustomerItemPricesSql);
        sql.append(" AND id = :partId");
        msps.addValue("customerId",customerId);
        jdbcTemplate.getNamedParameterJdbcOperations().query(queryCacheCustomerItemPricesSql,msps,
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        int id = rs.getInt("id");
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
        return jdbcTemplate.queryForObject(queryBasePartSql,new BeanPropertyRowMapper<Part>(Part.class),msps);
    }

    @Override
    public int getAccessCount(int customerId) {
        String getCatalogAccessCountSql =
                "SELECT count(*) FROM REQUEST_LOG WHERE CUSTOMER_ID = :customerId AND CREATE_TS > (CURRENT_TIMESTAMP - interval '24 hour') and STATUS = 'ACCEPTED'";
        MapSqlParameterSource msps = new MapSqlParameterSource();
        msps.addValue("customerId",customerId);
        return jdbcTemplate.queryForInt(getCatalogAccessCountSql, msps);
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
        StringBuffer rlrSql = new StringBuffer(requestLogReportSql);
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


}