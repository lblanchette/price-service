package com.suitesoftware.psa.catalogservice.impl;

import com.suitesoftware.psa.catalogservice.AccessException;
import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import org.springframework.transaction.annotation.Transactional;

/**
 * User: lrb
 * Date: 3/16/11
 * Time: 12:51 AM
 * (c) Copyright Suite Business Software
 */

public class AccessManagerImpl implements com.suitesoftware.psa.catalogservice.AccessManager {

    Integer accessLimit;  // 24 hour access limit

    String serviceAdminKey;

    CatalogDaoImpl catalogDao;


    public Integer getAccessLimit() {
        return accessLimit;
    }

    public void setAccessLimit(Integer accessLimit) {
        this.accessLimit = accessLimit;
    }

    public CatalogDaoImpl getCatalogDao() {
        return catalogDao;
    }

    public void setCatalogDao(CatalogDaoImpl psaCatalogDao) {
        this.catalogDao = psaCatalogDao;
    }

    public String getServiceAdminKey() {
        return serviceAdminKey;
    }

    public void setServiceAdminKey(String serviceAdminKey) {
        this.serviceAdminKey = serviceAdminKey;
    }


    /**
     * Grants access otherwise throws exception
     * @param customer
     * @param key
     * @throws Exception
     */
    @Override
    @Transactional (readOnly = true)
    public void grantCatalogAccess(CatalogCustomer customer, String key) throws AccessException {

        if(key.equals(customer.getPriceListAccessKey())) {
            if(getCatalogDao().getAccessCount(customer.getCustomerId()) > getAccessLimit()) {
                throw new AccessException("Access denied.  You have exceeded allowed usage for a 24 hour period.  customer ID: " + customer.getCustomerId());
            }
            return;
        }
        if(grantAdminAccess(key)){
            return;
        }
        throw new AccessException("Access denied. The access key provided is not authorized to access the requested resource");
    }

    @Override
    public boolean grantAdminAccess(String key) {
        return getServiceAdminKey().equals(key);
    }

}
