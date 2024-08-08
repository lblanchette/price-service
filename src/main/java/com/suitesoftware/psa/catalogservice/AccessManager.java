package com.suitesoftware.psa.catalogservice;

import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;

public interface AccessManager {

    void grantCatalogAccess(CatalogCustomer customer, String key) throws AccessException;

    boolean grantAdminAccess(String key);
}
