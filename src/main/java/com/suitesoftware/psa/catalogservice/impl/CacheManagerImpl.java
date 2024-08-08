package com.suitesoftware.psa.catalogservice.impl;

import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;

public class CacheManagerImpl implements com.suitesoftware.psa.catalogservice.CacheManager {

    private static final Logger log = LogManager.getLogger(CacheManagerImpl.class);

    CatalogDaoImpl catalogDao;

    public CatalogDaoImpl getCatalogDao() {
        return catalogDao;
    }

    public void setCatalogDao(CatalogDaoImpl catalogDao) {
        this.catalogDao = catalogDao;
    }

    @Override
    @Transactional
    public void updateCacheCustomers(String accountId) throws SQLException {
        try {
            log.info("upsertCacheCustomers");
            int changed = getCatalogDao().upsertCacheCustomers(accountId);
            log.info("upsertCacheCustomers changed: " + changed);
//            changed = getCatalogDao().discontinueCacheItems(accountId);
//            log.info("upsertCacheCustomers discontinued: " + changed);
//            changed = getCatalogDao().discontinueCustomerPrices(accountId);
//            log.info("upsertCacheCustomers discontinued: " + changed);
        } catch (Throwable ex) {
            log.error(ex.getMessage(),ex);
        }
    }


    @Override
    @Transactional
    public void updateCacheItems(String accountId) throws SQLException {
        try {
            log.info("upsertCacheItems");
            int changed = getCatalogDao().upsertCacheItems(accountId);
            log.info("upsertCacheItems changed: " + changed);
            changed = getCatalogDao().discontinueCacheItems(accountId);
            log.info("upsertCacheItems discontinued: " + changed);
            changed = getCatalogDao().discontinueCustomerPrices(accountId);
            log.info("discountinueCustomerPrices discontinued: " + changed);
        } catch (Throwable ex) {
            log.error(ex.getMessage(),ex);
        }
    }

    @Override
    @Transactional
    public void updateCacheCustomerPrices(CatalogCustomer catalogCustomer) throws SQLException {
        int changed = getCatalogDao().upsertCustomerPrices(catalogCustomer.getAccountId(),catalogCustomer.getCustomerId());
        log.info("upsertCustomerPrices changed: " + changed);
    }

}
