package com.suitesoftware.psa.catalogservice;

import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.dto.Part;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

public interface NsDao {

    void setDatasource(DataSource datasource);

    CatalogCustomer getCatalogCustomer(Integer customerId);

    List<CatalogCustomer> getCatalogCustomers();

    List<Part> getBaseParts();

    void assignCustomerPrices(Map<Integer, Part> partMap, int customerId);
}
