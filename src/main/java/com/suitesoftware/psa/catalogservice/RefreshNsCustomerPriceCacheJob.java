package com.suitesoftware.psa.catalogservice;

import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;

public interface RefreshNsCustomerPriceCacheJob {
    void startRefresh();
    void startRefresh(Integer customerId);
}
