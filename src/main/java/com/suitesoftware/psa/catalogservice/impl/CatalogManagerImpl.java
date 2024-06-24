package com.suitesoftware.psa.catalogservice.impl;

import com.suitesoftware.psa.catalogservice.*;
import com.suitesoftware.psa.catalogservice.dto.Catalog;
import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.dto.Part;
import com.suitesoftware.psa.catalogservice.dto.PartList;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.transaction.annotation.Transactional;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: lrb
 * Date: 3/15/11
 * Time: 1:26 PM
 * (c) Copyright Suite Business Software
 */
//@Transactional
public class CatalogManagerImpl implements CatalogManager {

    private Logger log = Logger.getLogger(getClass());

    @Autowired
    ApplicationContext applicationContext;

    CatalogDao catalogDao;

    NsDataSourceFactory nsDataSourceFactory;

    CacheFileManagerImpl cacheFileManager;

    RefreshNsCustomerPriceCacheJobImpl refreshNsCustomerPriceCacheJob;

    public RefreshNsCustomerPriceCacheJobImpl getRefreshNsCustomerPriceCacheJob() {
        return refreshNsCustomerPriceCacheJob;
    }

    public void setRefreshNsCustomerPriceCacheJob(RefreshNsCustomerPriceCacheJobImpl refreshNsCustomerPriceCacheJob) {
        this.refreshNsCustomerPriceCacheJob = refreshNsCustomerPriceCacheJob;
    }

    public NsDataSourceFactory getNsDataSourceFactory() {
        return nsDataSourceFactory;
    }

    public void setNsDataSourceFactory(NsDataSourceFactory nsDataSourceFactory) {
        this.nsDataSourceFactory = nsDataSourceFactory;
    }

    public CatalogDao getCatalogDao() {
        return catalogDao;
    }

    public void setCatalogDao(CatalogDao catalogDao) {
        this.catalogDao = catalogDao;
    }

    public CacheFileManagerImpl getCacheFileManager() {
        return cacheFileManager;
    }

    public void setCacheFileManager(CacheFileManagerImpl cacheFileManager) {
        this.cacheFileManager = cacheFileManager;
    }
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }


    public CatalogCustomer getCustomer(Integer customerId) {
        return getCatalogDao().getCustomer(customerId);
    }

    @Override
    @Transactional(readOnly = true)
    public StreamingOutput getCatalogOutputStream(CatalogCustomer customer, Date modifiedSince, String format, String useCache) {
        if(customer.isCaching() && !"F".equals(useCache) && cacheFileManager.getCustomerFile(customer.getCustomerId()).exists()) {
            return cacheFileManager.getCacheFileStreamingOutput(customer.getCustomerId(), modifiedSince);
        } else {
            return getUncachedCatalogOutputStream(customer,modifiedSince,format);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public StreamingOutput getUncachedCatalogOutputStream(CatalogCustomer customer, Date modifiedSince, String format) {
        StringBuilder errorBuilder = new StringBuilder();
        try {
            List<Part> parts = getCatalogDao().getCachePartsList(false);
            Map<Integer,Part> basePartsMap = new HashMap<Integer, Part>(parts.size());
            for(Part part : parts) {
                basePartsMap.put(part.getId(),part);
            }
            getCatalogDao().assignCacheCustomerPrices(basePartsMap, customer.getCustomerId());

            Catalog catalog = new Catalog();
            catalog.setCustomerId(customer.getCustomerId());
            if (modifiedSince != null) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                catalog.setModifiedSince(sdf.format(modifiedSince));
            }
            catalog.setPartList(new PartList());
            catalog.getPartList().setPart(parts);
            JAXBContext jaxbContext = JAXBContext.newInstance(Catalog.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
            return new StreamingOutput() {
                @Override
                public void write(OutputStream outputStream) throws IOException, WebApplicationException {
                    try {
                        jaxbMarshaller.marshal(catalog, outputStream);
                        log.info("filtered stream complete: " + modifiedSince);
                    } catch (Throwable ex) {
                        log.warn("Error marshalling", ex);
                        throw new IOException("Error marshalling", ex);
                    }
                }
            };
        } catch (Throwable ex) {
            errorBuilder.append("Error getCatalogOutputStream: ").append(ex.getMessage());
            log.warn("Error getCatalogOutputStream", ex);
        }
        // if error ...
        return new StreamingOutput() {
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {
                outputStream.write(errorBuilder.toString().getBytes(StandardCharsets.UTF_8));
                outputStream.flush();
            }
        };
    }

    /*

    // To be replaced
    @Transactional(readOnly = true)
    public Catalog cachedCatalogProducer(int customerId, Date modifiedSince) throws CatalogException {
        log.info("cachedCatalogProducer: " + customerId + " " + modifiedSince);
        try {
            Catalog cat = new Catalog();
            cat.setCustomerId(customerId);
            cat.setPartList(new PartList());
            cat.getPartList().setPart(getPsaCatalogDao().getCachedCustomerPartsList(customerId, modifiedSince));
            log.info("cachedCatalogProducer complete: " + customerId + " " + modifiedSince);
            return cat;
        } catch (Throwable ex) {
            log.info(ex.getMessage(),ex);
            throw new CatalogException(ex);
        }
    }
    */
/*
    // old not used
    @Transactional(readOnly = true)
    public Catalog catalogProducer(int customerId, Calendar modifiedSince) throws CatalogException {
        log.info("catalogProducer: " + customerId + " " + modifiedSince);
        try {
            List<Part> parts = getPsaCatalogDao().getBasePriceList();
            Map<Integer,BigDecimal> customerPriceMap = new HashMap<Integer, BigDecimal>(parts.size());
            getPsaCatalogDao().updateCustomerPriceMap(customerPriceMap, customerId);

            for(Part p : parts) {
                BigDecimal price = customerPriceMap.get(p.getId());
                if(price != null) {
                    p.setPrice(price);
                }
            }
            Catalog cat = new Catalog();
            cat.setCustomerId(customerId);
            cat.setPartList(new PartList());
            cat.getPartList().setPart(parts);
            log.info("catalogProducer complete: " + customerId + " " + modifiedSince);
            return cat;
        } catch (Throwable ex) {
            log.info(ex.getMessage(),ex);
            throw new CatalogException(ex);
        }
    }
*/
    /**
     * this is to be replaced
     */
/*
    @Transactional(readOnly = false)
    public void refreshCustomerPriceCache() throws CatalogException {

        log.info("refreshCustomerPriceCache");
        try {
            List<Integer> priceListCustomers = psaCatalogDao.getPriceListCustomersIds();
            List<Part> baseParts = getPsaCatalogDao().getBasePriceList();
            Map<Integer,Part> basePartsMap = new HashMap<Integer, Part>(baseParts.size());
            for(Part part : baseParts) {
                basePartsMap.put(part.getId(),part);
            }

            Map<Integer,BigDecimal> customerPriceMap = new HashMap<Integer, BigDecimal>(baseParts.size());

            for(Integer customerId : priceListCustomers) {
                log.info("Updating refreshCustomerPriceCache cust " + customerId);
                customerPriceMap.clear();
                log.info("Compute prices");
                getPsaCatalogDao().updateCustomerPriceMap(customerPriceMap, customerId);
                log.info("Update cache");
                getPsaCatalogDao().updateCustomerCacheParts(customerId, basePartsMap,customerPriceMap);
//                if(true) {
//                    break;
//                }
            }
            log.info("refreshCustomerPriceCache complete");
        } catch (Throwable ex) {
            log.info(ex.getMessage(),ex);
            throw new CatalogException(ex);
        }
        log.info("Updating refreshCustomerPriceCache complete");
    }
*/
    @Override
    @Transactional(readOnly = true)
    public Part getCustomerPart(Integer custoemrId, Integer partId) {
        return getCatalogDao().getCustomerPart(custoemrId, partId);
    }

    /*
    private String serviceKey = "bhnlwFAax3g6JQzZDamI";

    @Transactional(readOnly = true)
    public Part getCurrentPart(int customerId, int partId) throws Exception {

        Part part = getPsaCatalogDao().getBasePart(partId);

        URL url = new URL("https://forms.netsuite.com/app/site/hosting/scriptlet.nl?script=22&deploy=1&compid=1063595&h=e4a108215feaf0531576&service_key=" + serviceKey + "&cust_id=" + customerId + "&item_ids=" + partId);
        URLConnection urlCon = url.openConnection();
        byte [] bf = new byte[1024];
        InputStream is = urlCon.getInputStream();
        int len = 0;
        StringBuffer sb = new StringBuffer();
        while((len = is.read(bf)) != -1) {
            sb.append(new String(bf));
        }
        BufferedReader br = new BufferedReader(new StringReader(sb.toString()));
        String line = br.readLine();
        if(line.length() > 0) {
            String [] parts = line.split(",");
            part.setPrice(new BigDecimal(parts[1]));
        }
        return part;
    }
    */

    @Override
    @Transactional(readOnly = false)
    public void insertRequestLog(int customerId, String status, String response, int bytes) throws CatalogException {

        log.info("insertRequestLog");
        try {
            getCatalogDao().insertRequestLog(customerId,status,response,bytes);
            log.info("insertRequestLog complete");
        } catch (Throwable ex) {
            log.warn(ex.getMessage(),ex);
            throw new CatalogException(ex);
        }
        log.info("Updating insertRequestLog complete customer ID: " + customerId);
    }

    @Override
    public void refreshCustomerPrices(Integer customerId)  {
        try {
            log.info("Start refresh");
            RefreshNsCustomerPriceCacheJob job = applicationContext.getBean(RefreshNsCustomerPriceCacheJob.class);
            job.startRefresh(customerId);
        } catch(Throwable ex) {
            log.warn(ex.getMessage(),ex);
        }
    }
/*
    @Override
    public void refreshCustomers()  {
        try {
            log.info("Start refresh");

            RefreshNsCustomerPriceCacheJob job = applicationContext.getBean(RefreshNsCustomerPriceCacheJob.class);
            job.startRefresh();
        } catch(Throwable ex) {
            log.warn(ex.getMessage(),ex);
        }
    }
*/


    public void compareCustomerPrices()  {

        try {
            //List<Integer> customerIds = getCatalogDao().getPriceListCustomersIds();
            String baseDir = "/Users/lblanchette/dev/projects/price-service/extenserve/";

            List<Integer> customerIds = new LinkedList<>();
            customerIds.add(949);

            for (Integer customerId : customerIds) {
                System.out.println("Customer " + customerId);
                Catalog oldCatalog = cacheFileManager.getCustomerCatalog(customerId, new File(baseDir +customerId + "-ext.xml"));
                Catalog newCatalog = cacheFileManager.getCustomerCatalog(customerId);
                Map<Integer,Part> oldPartMap = new HashMap<>();
                for(Part part : oldCatalog.getPartList().getPart()) {
                    if(part.isDiscontinue()) {
                        continue;
                    }
                    oldPartMap.put(part.getId(),part);
                }
                if(oldPartMap.size() != newCatalog.getPartList().getPart().size()) {
                    System.out.println("Coart Counts differ old: " + oldPartMap.size()  +
                            " new " + newCatalog.getPartList().getPart().size());
                };

                for(Part newPart : newCatalog.getPartList().getPart()) {
                    if(newPart.getManPartNo() == null)
                        newPart.setManPartNo("");
                    if(newPart.getMan() == null)
                        newPart.setMan("");
                    if(newPart.getDesc() == null)
                        newPart.setDesc("");
                    if(newPart.getVendor() == null)
                        newPart.setVendor("");
                    Part oldPart = oldPartMap.get(newPart.getId());
                    if(oldPart == null || newPart == null) {
                        System.out.println("Obe is null");
                    }
                    try {
                        if (oldPart != null && oldPart.getMsrp() == null && newPart.getMsrp() != null)
                            if(newPart.getMsrp() == 0.0)
                                newPart.setMsrp(null);
                    } catch (Throwable ex) {
                        System.out.println("Huh is null");
                    }
                    if(newPart.getId() == 373353) {
                        System.out.println("here");
                    }
                    if (oldPart == null) {
                        System.out.println(newPart.getId() + ",old part not found");
                    } else if (oldPart.changed(newPart)) {
                        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
                        System.out.println("OLD:\n" + ow.writeValueAsString(oldPart));
                        System.out.println("NEW:\n" + ow.writeValueAsString(newPart));
                        //System.out.println(oldPart.getId() + "," + oldPart.getPrice().toPlainString() + "," + newPart.getPrice().toPlainString());
                    }
                }
            }
        } catch(Throwable ex) {
            ex.printStackTrace();
        }
    }

/*
    public void betaRefreshCustomerPrice(int customerId) throws Exception {

        try {
            //refreshCustomerPriceCacheJob.refreshCustomerPriceCache(customerId);
        } catch(Throwable ex) {
            ex.printStackTrace();
        }
    }
*/
    @Override
    @Transactional(readOnly = true)
    public String getAccessReport(Integer customerId, Date asOfDate) {
        StringBuilder sb = new StringBuilder();
        sb.append("request_date,request_time,customer_id,name,companyname\n");
        List<Map<String, Object>> rows = getCatalogDao().requestLogReport(customerId,asOfDate);
        for(Map<String, Object> row : rows) {
            sb.append(row.get("request_date")).append(",");
            sb.append(row.get("request_time")).append(",");
            sb.append(row.get("customer_id")).append(",");
            sb.append("\"").append(row.get("name_id")).append("\",");
            sb.append("\"").append(row.get("companyname")).append("\"\n");
        }
        return sb.toString();
    }

    public static void main(String [] args) {

        try {

            ApplicationContext ctx = new ClassPathXmlApplicationContext("applicationContext.xml");
            CatalogManager cm = (CatalogManager) ctx.getBean("catalogManager");

            //cm.refreshCustomerPrices(null);
            cm.compareCustomerPrices();

//            String report = cm.getAccessReport(null,null);
//            FileWriter fw = new FileWriter("log_report.csv");
//            fw.write(report);
//            fw.flush();
//            fw.close();
            //cm.refreshCustomerPriceCache();
            //cm.betaRefreshCustomerPrices();
            //cm.compareCustomerPrices();
            /*
            StreamingOutput so = cm.getCatalogOutputStream(949);
            FileOutputStream fos = new FileOutputStream("st.xml");
            so.write(fos);
            System.out.println("done");
             */

//            PsaCatalogDao dao = cm.getPsaCatalogDao();
//            List<Part> partList = dao.getCachedCustomerPart(968,8);
//
//            int i = 0;


        } catch(Throwable ex) {
            ex.printStackTrace();
        }
    }
}
