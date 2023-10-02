package com.suitesoftware.psa.catalogservice.impl;

import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import org.apache.log4j.Logger;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;

public class CacheManagerImpl implements com.suitesoftware.psa.catalogservice.CacheManager {

    private Logger log = Logger.getLogger(getClass());

//    @Autowired
//    private PlatformTransactionManager platformTransactionManager;

    CatalogDaoImpl catalogDao;

    public CatalogDaoImpl getCatalogDao() {
        return catalogDao;
    }

    public void setCatalogDao(CatalogDaoImpl catalogDao) {
        this.catalogDao = catalogDao;
    }

    @Override
    @Transactional
    public void updateCacheItems() throws SQLException {
        try {
            log.info("upsertCacheItems");
            int changed = getCatalogDao().upsertCacheItems();
            log.info("upsertCacheItems changed: " + changed);
            changed = getCatalogDao().discontinueCacheItems();
            log.info("upsertCacheItems discontinued: " + changed);
            changed = getCatalogDao().discontinueCustomerPrices();
            log.info("discountinueCustomerPrices discontinued: " + changed);
            //DataSourceUtils.getConnection(getDataSource()).commit();
        } catch (Throwable ex) {
            log.error(ex.getMessage(),ex);
        }
    }

    @Override
    @Transactional
    public void updateCacheCustomerPrices(CatalogCustomer catalogCustomer) throws SQLException {
        int changed = getCatalogDao().upsertCustomerPrices(catalogCustomer.getCustomerId());
        log.info("upsertCustomerPrices changed: " + changed);
    }

}
