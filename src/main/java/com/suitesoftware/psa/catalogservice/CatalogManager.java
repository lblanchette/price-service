package com.suitesoftware.psa.catalogservice;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.annotation.Transactional;
import sun.net.www.http.HttpClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: lrb
 * Date: 3/15/11
 * Time: 1:26 PM
 * (c) Copyright Suite Business Software
 */
@Transactional

public class CatalogManager {

    private Logger log = Logger.getLogger(getClass());

    PsaCatalogDao psaCatalogDao;

    public PsaCatalogDao getPsaCatalogDao() {
        return psaCatalogDao;
    }

    public void setPsaCatalogDao(PsaCatalogDao psaCatalogDao) {
        this.psaCatalogDao = psaCatalogDao;
    }

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
            MapSqlParameterSource params = new MapSqlParameterSource();
            params.addValue("customerId",customerId);
            cat.getPartList().setPart(parts);
            log.info("catalogProducer complete: " + customerId + " " + modifiedSince);
            return cat;
        } catch (Throwable ex) {
            log.info(ex.getMessage(),ex);
            throw new CatalogException(ex);
        }
    }

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
            }
            log.info("refreshCustomerPriceCache complete");
        } catch (Throwable ex) {
            log.info(ex.getMessage(),ex);
            throw new CatalogException(ex);
        }
        log.info("Updating refreshCustomerPriceCache complete");
    }


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


    public static void main(String [] args) {

        try {

            ApplicationContext ctx = new ClassPathXmlApplicationContext("applicationContext.xml");
            CatalogManager cm = (CatalogManager) ctx.getBean("catalogManager");
            PsaCatalogDao dao = cm.getPsaCatalogDao();
            List<Part> partList = dao.getCachedCustomerPart(968,8);

            int i = 0;


        } catch(Throwable ex) {
            ex.printStackTrace();
        }



    }

}
