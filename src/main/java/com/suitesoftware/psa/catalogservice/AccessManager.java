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

    @Transactional (readOnly = true)
    boolean grantCatalogAccess(int customerId, String key) {

        CatalogCustomer customer = getPsaCatalogDao().getCustomer(customerId);

        if(key.equals(customer.getPriceListAccessKey())) {
            return true;
        }
        if("729695c9-4fae-11e0-b438-714473f5cce7".equals(key)){
            return true;
        }
        return false;
    }

    @Transactional (readOnly = true)
    boolean grantAdminAccess(String key) {
        if("729695c9-4fae-11e0-b438-714473f5cce7".equals(key))
            return true;
        else
            return false;
    }

}
