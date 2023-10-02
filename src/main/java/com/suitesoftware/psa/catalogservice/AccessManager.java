package com.suitesoftware.psa.catalogservice;

import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import org.springframework.transaction.annotation.Transactional;

public interface AccessManager {

    void grantCatalogAccess(CatalogCustomer customer, String key) throws AccessException;

    boolean grantAdminAccess(String key);
}
