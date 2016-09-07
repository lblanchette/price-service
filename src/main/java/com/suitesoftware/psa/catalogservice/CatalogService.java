package com.suitesoftware.psa.catalogservice;



import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.net.httpserver.HttpServer;
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

    private Logger log = Logger.getLogger(getClass());

    private CatalogManager catalogManager;
    private AccessManager accessManager;

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    @Autowired
    public void setCatalogManager(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public AccessManager getAccessManager() {
        return accessManager;
    }

    @Autowired
    public void setAccessManager(AccessManager accessManager) {
        this.accessManager = accessManager;
    }

    @GET
    @Path("{customerId : \\d+}")
    @Produces("application/xml")
    public Catalog getCatalog(@PathParam("customerId") int customerId, @QueryParam("modified-since") String modifiedSinceStr, @QueryParam("access-key") String accessKey, @HeaderParam("x-psa-access-key") String headerAccessKey) throws Exception {
        try {
            log.info("getCatalog AccessKey:" + accessKey + "HeaderAccessKey:" + headerAccessKey +" CustomerId:" + customerId);
            if(accessKey == null && headerAccessKey == null) {
                throw new Exception("Access key is required");
            }
            if(!getAccessManager().grantCatalogAccess(customerId,accessKey != null ? accessKey : headerAccessKey)) {
                throw new Exception("Access denied.  The access key provided is not authorized to access the requested resource");
            }
            Date modifiedSince = null;
            if(modifiedSinceStr != null && modifiedSinceStr.length() > 0) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                modifiedSince = sdf.parse(modifiedSinceStr);
            }
            return   getCatalogManager().cachedCatalogProducer(customerId, modifiedSince);
            //return   getCatalogManager().catalogProducer(customerId, null);
        } catch (Throwable ex) {
            log.error("Error processing request: " + ex.getMessage(),ex);
            throw new Exception("Error processing request: " + ex.getMessage(),ex);
        }
    }


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


    @GET
    @Path("part/{customerId : \\d+}/{partId : \\d+}")
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
        return getCatalogManager().getCurrentPart(customerId, partId);
    }

    private Part partProducer(int customerId, int partId) {
        Part p = new Part();
        p.setId(89653);
        p.setPartNo("SRD-850D-1647");
        p.setDesc("DVR, 500 GB, 8-Channel, 60 FPS @ 4CIF, 120 FPS @ 2CIF, 240 FPS @");
        p.setDiscontinue(false);
        p.setLastModified(new Date());
        p.setMan("Samsung Techwin America");
        p.setManPartNo("SRD-850D");
        p.setMsrp(new BigDecimal("1700.00"));
        p.setPrice(new BigDecimal("765.00"));
        p.setVendor("PSA");
        return p;
    }


    public static void main(String[] args) throws IOException
        {
        HttpServer server = HttpServerFactory.create("http://localhost:9998/");
        server.start();

        System.out.println("Server running");
        System.out.println("Visit: http://localhost:9998/catalog");
        System.out.println("Hit return to stop...");
        System.in.read();
        System.out.println("Stopping server");
        server.stop(0);
        System.out.println("Server stopped");
    }
}



