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

    List<CatalogCustomer> getCatalogCustomers();


    CatalogCustomer getCustomer(int customerId);

    List<Part> getCacheCustomerParts(int customerId);

    List<Part> getCachePartsList(Boolean discontinue);

    int upsertCacheItems();

    int discontinueCacheItems();

    int upsertCustomerPrices(int customerId);

    int discontinueCustomerPrices();

    void assignCacheCustomerPrices(Map<Integer, Part> basePartsMap, int customerId);

    Part getCustomerPart(int customerId, int partId);

    Part getBasePart(int partId);

    int getAccessCount(int customerId);

    int insertRequestLog(int customerId, String status, String response, int bytes);

    List<Map<String, Object>> requestLogReport(Integer customerId, Date asOfDate);
}
