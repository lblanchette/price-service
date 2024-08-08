package com.suitesoftware.psa.catalogservice;

import com.suitesoftware.psa.catalogservice.dto.Catalog;
import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.dto.Part;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

public interface CatalogDao {
    void deleteCustomerPrices(int customerId);

    void insertBasePrices(int customerId);

    void updateCustomerItemPrices(int customerId);

    void updateCustomerGroupPrices(int customerId);

    void updateCustomerLevelPrices(Integer customerId);

    Catalog getCustomerCatalog(int customerId, Calendar modifiedSince);

    void updateCustomerPriceMap(Map<Integer, BigDecimal> customerPriceMap, int customerId);

    void updateCustomerPriceMapNew(Map<Integer, Part> basePartsMap, int customerId);

    void streamCachedCustomerPartsList(OutputStream outputStream, int customerId, Date lastModified);

    List<Part> getCachedCustomerPartsList(int customerId, Date modifiedSince);

    List<Part> getBasePriceList() throws Exception;

    List<Integer> getPriceListCustomersIds() throws Exception;

    void updateCustomerCacheParts(int customerId, Map<Integer, Part> basePartsMap, Map<Integer, BigDecimal> customerPriceMap);

    //List<CatalogCustomer> getCatalogCustomers();

    List<CatalogCustomer> getKeyedCatalogCustomers(String accountId);
    List<CatalogCustomer> getCatalogCustomers(String accountId);
    CatalogCustomer getCatalogCustomer(String accountId, int customerId);

    //List<Part> getCacheCustomerParts(String accountId, int customerId);

    List<Part> getCachePartsList(String accountId, Boolean discontinue);

    int upsertCacheCustomers(String accountId);

    int upsertCacheItems(String accountId);

    int discontinueCacheItems(String accountId);

    int upsertCustomerPrices(String accountId, int customerId);

    int discontinueCustomerPrices(String accountId);

    void assignCacheCustomerPrices(Map<Integer, Part> basePartsMap, String accountId, int customerId);

    Part getCustomerPart(String accountId, int customerId, int partId);

    Part getBasePart(int partId);

    Integer getAccessCount(int customerId);

    int insertRequestLog(int customerId, String status, String response, int bytes);

    List<Map<String, Object>> requestLogReport(Integer customerId, Date asOfDate);
}
