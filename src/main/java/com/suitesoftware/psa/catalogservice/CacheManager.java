package com.suitesoftware.psa.catalogservice;

import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;

public interface CacheManager {

    void updateCacheItems() throws SQLException;

    void updateCacheCustomerPrices(CatalogCustomer catalogCustomer) throws SQLException;
}
