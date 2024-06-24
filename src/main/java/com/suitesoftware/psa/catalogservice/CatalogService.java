package com.suitesoftware.psa.catalogservice;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.JAXB;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import com.suitesoftware.psa.catalogservice.dto.Catalog;
import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.dto.Part;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * User: lrb
 * Date: 2/17/11
 * Time: 2:14 PM
 * (c) Copyright Suite Business Software
 */

@Path("/catalog")

@Component
@Scope("request")

public class CatalogService {

    private final Logger log = Logger.getLogger(getClass());

    private CatalogManager catalogManager;
    private NsCatalogManager nsCatalogManager;
    private AccessManager accessManager;

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    @Autowired
    public void setCatalogManager(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }


    public NsCatalogManager getNsCatalogManager() {
        return nsCatalogManager;
    }

    public void setNsCatalogManager(NsCatalogManager nsCatalogManager) {
        this.nsCatalogManager = nsCatalogManager;
    }

    public AccessManager getAccessManager() {
        return accessManager;
    }

    @Autowired
    public void setAccessManager(AccessManager accessManager) {
        this.accessManager = accessManager;
    }

    private Date dateStringToDate(String dateStr) throws Exception {

        String dateFmt = "yyyy-MM-dd";
        if (dateStr != null) {
            if(dateStr.length() != dateFmt.length()) {
                throw new Exception("Date format must be '" + dateFmt + "'");
            }
            SimpleDateFormat sdf = new SimpleDateFormat(dateFmt);
            return sdf.parse(dateStr);
        }
        return null;
    }

    @GET
    @Path("{customerId : \\d+}")
    @Produces("application/xml")

    public Response getCatalog(@PathParam("customerId") int customerId, @QueryParam("modified-since") String modifiedSinceStr, @QueryParam("use-cache") String useCache, @QueryParam("format") String format, @QueryParam("access-key") String accessKey, @HeaderParam("x-psa-access-key") String headerAccessKey) throws Exception {

        log.info("getCatalog AccessKey:" + accessKey + "HeaderAccessKey:" + headerAccessKey +" CustomerId:" + customerId);

        StringBuilder errorBuilder = new StringBuilder();
        try {
            if (accessKey == null && headerAccessKey == null) {
                log.warn("Access key is required CustomerId: " + customerId);
                throw new Exception("Access key is required");
            }
            CatalogCustomer catalogCustomer = getCatalogManager().getCustomer(customerId);
            getAccessManager().grantCatalogAccess(catalogCustomer, accessKey != null ? accessKey : headerAccessKey);
            getCatalogManager().insertRequestLog(customerId,"ACCEPTED","",0);
            Date modifiedSince = dateStringToDate(modifiedSinceStr);
            MediaType mediaType;
            if(format == null || format.equals("XML")) {
                mediaType = MediaType.APPLICATION_XML_TYPE;
            } else {
                throw new Exception("Invalid format specified. Must be 'XML', 'XLS'");
            }
            //Response.ok("", mediaType);
            return Response.ok(getCatalogManager().getCatalogOutputStream(catalogCustomer, modifiedSince, format, useCache),mediaType).build();
        } catch (AccessException ex) {
            try {
                getCatalogManager().insertRequestLog(customerId,"DENIED",ex.getMessage(),0);
            } catch (Throwable aex) {
                log.warn("Insert request log error: " + ex.getMessage());
            }
            log.info("AccessException getCatalog: " + ex.getMessage());
            errorBuilder.append("Error: " + ex.getMessage());
        } catch (Throwable ex) {
            log.warn("Error getCatalog", ex);
            errorBuilder.append("Error: " + ex.getMessage());
        }
        // otherwise return XML with error message
        Catalog catalog = new Catalog();
        catalog.setMessage(errorBuilder.toString());
        return Response.ok(new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {
                try {
                    JAXB.marshal(catalog,outputStream);
                } catch (Throwable ex) {
                    log.warn("Error marshalling", ex);
                    throw new IOException("Error marshalling", ex);
                }
            }
        }).build();
    }
/*
    @GET
    @Path("{customerId : \\d+}")
    @Produces("application/xml")

    public Catalog getCatalog(@PathParam("customerId") int customerId, @QueryParam("modified-since") String modifiedSinceStr, @QueryParam("access-key") String accessKey, @HeaderParam("x-psa-access-key") String headerAccessKey) throws Exception {
        try {
            log.info("getCatalog AccessKey:" + accessKey + "HeaderAccessKey:" + headerAccessKey +" CustomerId:" + customerId);
            if(accessKey == null && headerAccessKey == null) {
                log.warn("Access key is required CustomerId: " + customerId);
                Catalog catalog = new Catalog();
                catalog.setMessage("Access key is required");
                return catalog;
                //throw new Exception("Access key is required");
            }
            try {
                getAccessManager().grantCatalogAccess(customerId, accessKey != null ? accessKey : headerAccessKey);
            } catch (Exception ex) {
                Catalog catalog = new Catalog();
                catalog.setMessage(ex.getMessage());
                return catalog;
            }
            getCatalogManager().insertRequestLog(customerId,"ACCEPTED","",0);
            Date modifiedSince = null;
            if(modifiedSinceStr != null && modifiedSinceStr.length() > 0) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                modifiedSince = sdf.parse(modifiedSinceStr);
            }
            return   getCatalogManager().cachedCatalogProducer(customerId, modifiedSince);
        } catch (Throwable ex) {
            log.error("Error processing request: " + ex.getMessage(),ex);
            throw new Exception("Error processing request: " + ex.getMessage(),ex);
        }
    }

*/
    /**
     * this is to be replaced with betaRefreshCustomerPrices
     */
/*
    @GET
    @Path("admin/refresh/customerPrices")
    @Produces("text/plain")
    public String refreshCustomerPrices(@QueryParam("access-key") String accessKey, @HeaderParam("x-psa-access-key") String headerAccessKey) throws Exception {
        try {
            log.info("getCatalog AccessKey:" + accessKey + "HeaderAccessKey:" + headerAccessKey );
            if(accessKey == null && headerAccessKey == null) {
                throw new Exception("Access key is required");
            }
            if(!getAccessManager().grantAdminAccess(accessKey != null ? accessKey : headerAccessKey)) {
                throw new Exception("Access denied.  The access key provided is not authorized to access the requested resource");
            }
            getCatalogManager().refreshCustomerPriceCache();
            return "OK";
        } catch (Throwable ex) {
            log.error("Error processing request: " + ex.getMessage(),ex);
            throw new Exception("Error processing request: " + ex.getMessage(),ex);
        }
    }
*/
    @GET
    @Path("admin/refresh/customerPrices")
    @Produces("text/plain")
    public String customerPrices(@QueryParam("access-key") String accessKey, @HeaderParam("x-psa-access-key") String headerAccessKey) throws Exception {
        try {
            log.info("getCatalog AccessKey:" + accessKey + "HeaderAccessKey:" + headerAccessKey );
            if(accessKey == null && headerAccessKey == null) {
                throw new Exception("Access key is required");
            }
            if(!getAccessManager().grantAdminAccess(accessKey != null ? accessKey : headerAccessKey)) {
                throw new Exception("Access denied.  The access key provided is not authorized to access the requested resource");
            }
            getCatalogManager().refreshCustomerPrices(null);
            return "OK\n";
        } catch (Throwable ex) {
            log.error("Error processing request: " + ex.getMessage(),ex);
            throw new Exception("Error processing request: " + ex.getMessage(),ex);
        }
    }

/*
    @GET
    @Path("admin/refresh/customerPrice/{customerId : \\d+}")
    @Produces("text/plain")
    public String refreshCustomerPrice(@PathParam("customerId") int customerId, @QueryParam("access-key") String accessKey, @HeaderParam("x-psa-access-key") String headerAccessKey) throws Exception {
        try {
            log.info("getCatalog AccessKey:" + accessKey + "HeaderAccessKey:" + headerAccessKey );
            if(accessKey == null && headerAccessKey == null) {
                throw new Exception("Access key is required");
            }
            if(!getAccessManager().grantAdminAccess(accessKey != null ? accessKey : headerAccessKey)) {
                throw new Exception("Access denied.  The access key provided is not authorized to access the requested resource");
            }
            getCatalogManager().refreshCustomerPriceCache();
            return "OK";
        } catch (Throwable ex) {
            log.error("Error processing request: " + ex.getMessage(),ex);
            throw new Exception("Error processing request: " + ex.getMessage(),ex);
        }
    }
*/
    @GET
    @Path("admin/refresh/customerPrice/{customerId : \\d+}")
    @Produces("text/plain")
    public String betaRefreshCustomerPrice(@PathParam("customerId") int customerId, @QueryParam("access-key") String accessKey, @HeaderParam("x-psa-access-key") String headerAccessKey) throws Exception {
        try {
            log.info("getCatalog AccessKey:" + accessKey + "HeaderAccessKey:" + headerAccessKey );
            if(accessKey == null && headerAccessKey == null) {
                throw new Exception("Access key is required");
            }
            if(!getAccessManager().grantAdminAccess(accessKey != null ? accessKey : headerAccessKey)) {
                throw new Exception("Access denied.  The access key provided is not authorized to access the requested resource");
            }
            getCatalogManager().refreshCustomerPrices(customerId);
            return "OK\n";
        } catch (Throwable ex) {
            log.error("Error processing request: " + ex.getMessage(),ex);
            throw new Exception("Error processing request: " + ex.getMessage(),ex);
        }
    }
/*
    @GET
    @Path("admin/refresh/customers")
    @Produces("text/plain")
    public String refreshCustomers( @QueryParam("access-key") String accessKey, @HeaderParam("x-psa-access-key") String headerAccessKey) throws Exception {
        try {
            log.info("getCatalog AccessKey:" + accessKey + "HeaderAccessKey:" + headerAccessKey );
            if(accessKey == null && headerAccessKey == null) {
                throw new Exception("Access key is required");
            }
            if(!getAccessManager().grantAdminAccess(accessKey != null ? accessKey : headerAccessKey)) {
                throw new Exception("Access denied.  The access key provided is not authorized to access the requested resource");
            }
            getCatalogManager().refreshCustomers();
            return "OK\n";
        } catch (Throwable ex) {
            log.error("Error processing request: " + ex.getMessage(),ex);
            throw new Exception("Error processing request: " + ex.getMessage(),ex);
        }
    }
*/

    @GET
    @Path("{customerId : \\d+}/part/{partId : \\d+}")
    @Produces("application/xml")
    public Part getCatalogPart(@PathParam("customerId") int customerId, @PathParam("partId") int partId, @QueryParam("access-key") String accessKey, @HeaderParam("x-psa-access-key") String headerAccessKey) throws Exception {
        log.info("getCatalogPart AccessKey:" + accessKey + "HeaderAccessKey:" + headerAccessKey +  " CustomerId:" + customerId + " PartId:" + partId);

        log.info("getCatalog AccessKey:" + accessKey + "HeaderAccessKey:" + headerAccessKey );
        if(accessKey == null && headerAccessKey == null) {
            throw new Exception("Access key is required");
        }
        if(!getAccessManager().grantAdminAccess(accessKey != null ? accessKey : headerAccessKey)) {
            throw new Exception("Access denied.  The access key provided is not authorized to access the requested resource");
        }
        return getCatalogManager().getCustomerPart(customerId, partId);
    }

    @GET
    @Path("reports/access")
    @Produces("application/xml")
    public String getAccessReport(@PathParam("customerId") Integer customerId, @QueryParam("as-of-date") String asOfDateStr, @QueryParam("access-key") String accessKey, @HeaderParam("x-psa-access-key") String headerAccessKey) throws Exception {

        log.info("getAccessReport AccessKey:" + accessKey + "HeaderAccessKey:" + headerAccessKey);
        if(accessKey == null && headerAccessKey == null) {
            throw new Exception("Access key is required");
        }
        if(!getAccessManager().grantAdminAccess(accessKey != null ? accessKey : headerAccessKey)) {
            throw new Exception("Access denied.  The access key provided is not authorized to access the requested resource");
        }
        Date asOfDate = dateStringToDate(asOfDateStr);

        return getCatalogManager().getAccessReport(customerId,asOfDate);
    }

    @GET
    @Path("reports/access/{customerId : \\d+}")
    @Produces("application/xml")
    public String getCustomerAccessReport(@PathParam("customerId") Integer customerId, @QueryParam("as-of-date") String asOfDateStr, @QueryParam("access-key") String accessKey, @HeaderParam("x-psa-access-key") String headerAccessKey) throws Exception {
        return getAccessReport(customerId, asOfDateStr, accessKey,headerAccessKey );
    }


    /*

    curl "http://localhost:8080/price-service/catalog/admin/refresh/customerPrice/949?access-key=729695c9-4fae-11e0-b438-714473f5cce7"

    curl "http://localhost:8080/price-service/catalog/admin/refresh/customerPrices?access-key=729695c9-4fae-11e0-b438-714473f5cce7"

    curl "http://localhost:8080/price-service/catalog/reports/access?access-key=729695c9-4fae-11e0-b438-714473f5cce7"

    curl "http://localhost:8080/price-service/catalog/reports/access/949?access-key=729695c9-4fae-11e0-b438-714473f5cce7&as-of-date=2023-05-01"


    curl "http://localhost:8080/price-service/catalog/949/part/11?access-key=729695c9-4fae-11e0-b438-714473f5cce7&as-of-date=2023-05-01"


     */
}



