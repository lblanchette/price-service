package com.suitesoftware.psa.catalogservice;

import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.dto.Part;
import org.springframework.transaction.annotation.Transactional;

import jakarta.ws.rs.core.StreamingOutput;
import java.util.Date;

public interface CatalogManager {
    StreamingOutput getCatalogOutputStream(CatalogCustomer customer, Date modifiedSince, String format, String useCache);

    StreamingOutput getUncachedCatalogOutputStream(CatalogCustomer customer, Date modifiedSince, String format);

    CatalogCustomer getCatalogCustomer(String accountId, Integer custoemrId);

    Part getCustomerPart(String accountId, Integer customerId, Integer partId);

    void insertRequestLog(int customerId, String status, String response, int bytes) throws CatalogException;

    void refreshCustomerPrices(Integer customerId) throws Exception;

    void refreshCustomers() throws Exception;

    String getAccessReport(Integer customerId, Date asOfDate);

    void compareCustomerPrices();
}
