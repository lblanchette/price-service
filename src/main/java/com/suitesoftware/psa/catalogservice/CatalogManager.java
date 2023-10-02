package com.suitesoftware.psa.catalogservice;

import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.dto.Part;
import org.springframework.transaction.annotation.Transactional;

import javax.ws.rs.core.StreamingOutput;
import java.util.Date;

public interface CatalogManager {
    StreamingOutput getCatalogOutputStream(CatalogCustomer customer, Date modifiedSince, String format, String useCache);

    StreamingOutput getUncachedCatalogOutputStream(CatalogCustomer customer, Date modifiedSince, String format);

    CatalogCustomer getCustomer(Integer custoemrId);

    Part getCustomerPart(Integer custoemrId, Integer partId);

    void insertRequestLog(int customerId, String status, String response, int bytes) throws CatalogException;

    void refreshCustomerPrices(Integer customerId) throws Exception;

    String getAccessReport(Integer customerId, Date asOfDate);

    void compareCustomerPrices();
}
