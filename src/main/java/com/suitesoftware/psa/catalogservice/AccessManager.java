package com.suitesoftware.psa.catalogservice;

import org.springframework.transaction.annotation.Transactional;

/**
 * User: lrb
 * Date: 3/16/11
 * Time: 12:51 AM
 * (c) Copyright Suite Business Software
 */
@Transactional
public class AccessManager {

    PsaCatalogDao psaCatalogDao;

    public PsaCatalogDao getPsaCatalogDao() {
        return psaCatalogDao;
    }

    public void setPsaCatalogDao(PsaCatalogDao psaCatalogDao) {
        this.psaCatalogDao = psaCatalogDao;
    }

    @Transactional (readOnly = true, rollbackFor = Exception.class)

    void grantCatalogAccess(int customerId, String key) throws Exception {

        CatalogCustomer customer = getPsaCatalogDao().getCustomer(customerId);

        if(key.equals(customer.getPriceListAccessKey())) {
            if(getPsaCatalogDao().getAccessCount(customerId) > 10) {
                throw new Exception("Access denied.  You have exceeded allowed usage for a 24 hour period.  customer ID: " + customerId);
            }
            return;
        }
        if("729695c9-4fae-11e0-b438-714473f5cce7".equals(key)){
            return;
        }
        throw new Exception("Access denied. The access key provided is not authorized to access the requested resource");
/*
        CatalogCustomer customer = getPsaCatalogDao().getCustomer(customerId);

        if(key.equals(customer.getPriceListAccessKey())) {
            return true;
        }
        if("729695c9-4fae-11e0-b438-714473f5cce7".equals(key)){
            return true;
        }
        return false;
 */
    }

    @Transactional (readOnly = true)
    boolean grantAdminAccess(String key) {
        if("729695c9-4fae-11e0-b438-714473f5cce7".equals(key))
            return true;
        else
            return false;
    }

}
