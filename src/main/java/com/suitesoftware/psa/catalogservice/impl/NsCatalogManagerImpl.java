package com.suitesoftware.psa.catalogservice.impl;

import com.suitesoftware.psa.catalogservice.NsCatalogManager;
import com.suitesoftware.psa.catalogservice.NsDao;
import com.suitesoftware.psa.catalogservice.NsDataSourceFactory;
import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.dto.Part;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NsCatalogManagerImpl implements NsCatalogManager {

    private final Logger log = Logger.getLogger(getClass());
    NsDataSourceFactory nsDataSourceFactory;

    NsDao nsDao;

    public NsDataSourceFactory getNsDataSourceFactory() {
        return nsDataSourceFactory;
    }

    public void setNsDataSourceFactory(NsDataSourceFactory nsDataSourceFactory) {
        this.nsDataSourceFactory = nsDataSourceFactory;
    }

    public NsDao getNsDao() {
        return nsDao;
    }

    public void setNsDao(NsDao nsDao) {
        this.nsDao = nsDao;
    }

    @Override
    public CatalogCustomer getCatalogCustomer(Integer customerId) {

        try {
            nsDao.setDatasource(nsDataSourceFactory.newAdminNsDataSource());
            return nsDao.getCatalogCustomer(customerId);
        } catch (Throwable ex) {
            log.warn(ex);
        }
        return null;
    }

    @Override
    public List<Part> getBaseParts() {

        try {
            nsDao.setDatasource(nsDataSourceFactory.newAdminNsDataSource());
            return nsDao.getBaseParts();
        } catch (Throwable ex) {
            log.warn(ex);
        }
        return null;
    }

    @Override
    public List<Part> getCustomerParts(int customerId) {

        try {
            log.debug("getBaseParts");
            long start = System.currentTimeMillis();
            nsDao.setDatasource(nsDataSourceFactory.newAdminPooledNsDataSource(1));
            List<Part> parts = nsDao.getBaseParts();
            log.debug("getBaseParts elapsed: " + (System.currentTimeMillis() - start) + "ms");
            start = System.currentTimeMillis();
            Map<Integer,Part> partMap = new HashMap<>(parts.size());
            for(Part part : parts) {
                partMap.put(part.getId(),part);
            }
            log.debug("hash Parts: " + (System.currentTimeMillis() - start) + "ms");
            start = System.currentTimeMillis();
            nsDao.assignCustomerPrices(partMap, customerId);
            log.debug("assignCustomerPrices elapsed: " + (System.currentTimeMillis() - start));
            return parts;
        } catch (Throwable ex) {
            log.warn(ex);
        }
        return null;
    }

//    @Override
//    public void generateCustomerCatalogCache() {
//        try {
//            nsDao.setDatasource(nsDataSourceFactory.newAdminNsDataSource());
//            List<CatalogCustomer> catalogCustomers = nsDao.getCatalogCustomers();
//            log.debug("catalogCustomers count: " + catalogCustomers.size());
//            for(CatalogCustomer cust : catalogCustomers) {
//
//            }
//        } catch (Throwable ex) {
//            log.warn(ex);
//        }
//
//
//    }


    public static void main(String [] args) {

        try {

            ApplicationContext ctx = new ClassPathXmlApplicationContext("applicationContext.xml");
            NsCatalogManager cm = (NsCatalogManager) ctx.getBean("nsCatalogManager");

            //cm.getCatalogCustomer(949);
            //List<Part> baseParts = cm.getBaseParts();
            //List<Part> parts = cm.getCustomerParts(949);

//            cm.generateCustomerCatalogCache();


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
