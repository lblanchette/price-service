package com.suitesoftware.psa.catalogservice;

import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.dto.Part;

import java.util.List;

public interface NsCatalogManager {

    CatalogCustomer getCatalogCustomer(Integer customerId);

    List<Part> getBaseParts();

    List<Part> getCustomerParts(int customerId);

    //    void generateCustomerCatalogCache();
}
