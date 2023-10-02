package com.suitesoftware.psa.catalogservice.impl;

import com.suitesoftware.dbcopy.CopyDef;
import com.suitesoftware.dbcopy.CopyProcessor4;
import com.suitesoftware.psa.catalogservice.CacheManager;
import com.suitesoftware.psa.catalogservice.CatalogDao;
import com.suitesoftware.psa.catalogservice.NsDataSourceFactory;
import com.suitesoftware.psa.catalogservice.RefreshNsCustomerPriceCacheJob;
import com.suitesoftware.psa.catalogservice.dto.Catalog;
import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.dto.Part;
import com.suitesoftware.psa.catalogservice.dto.PartList;
import org.apache.log4j.Logger;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class RefreshNsCustomerPriceCacheJobImpl implements RefreshNsCustomerPriceCacheJob {

    private final Logger log = Logger.getLogger(getClass());

    private static final int NS_DB_POOL_SIZE = 1;
    private static final int REFRESH_THREADS = 1;

    private TaskExecutor refreshTaskExecutor;

    //private ThreadPoolTaskExecutor customerPricesRefreshTaskExecutor;

    NsDataSourceFactory nsDataSourceFactory;
    DataSource dataSource;

    CatalogDao catalogDao;

    CacheFileManagerImpl cacheFileManager;

    CacheManager cacheManager;

    CopyDef customerCopyDef;

    CopyDef itemCopyDef;

    CopyDef customerPriceCopyDef;

    public TaskExecutor getRefreshTaskExecutor() {
        return refreshTaskExecutor;
    }

    public void setRefreshTaskExecutor(TaskExecutor refreshTaskExecutor) {
        this.refreshTaskExecutor = refreshTaskExecutor;
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
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

    public CopyDef getCustomerCopyDef() {
        return customerCopyDef;
    }

    public void setCustomerCopyDef(CopyDef customerCopyDef) {
        this.customerCopyDef = customerCopyDef;
    }

    public CopyDef getItemCopyDef() {
        return itemCopyDef;
    }

    public void setItemCopyDef(CopyDef itemCopyDef) {
        this.itemCopyDef = itemCopyDef;
    }

    public CopyDef getCustomerPriceCopyDef() {
        return customerPriceCopyDef;
    }

    public void setCustomerPriceCopyDef(CopyDef customerPriceCopyDef) {
        this.customerPriceCopyDef = customerPriceCopyDef;
    }

//    public RefreshNsCustomerPriceCacheJob(NsDao nsDao, CatalogDao catalogDao, CacheFileManager cacheFileManager) {
//        this.nsDao = nsDao;
//        this.catalogDao = catalogDao;
//        this.cacheFileManager = cacheFileManager;
//        customerId = null;
//
//    }
//
//    public RefreshNsCustomerPriceCacheJob(NsDao nsDao, CatalogDao catalogDao, CacheFileManager cacheFileManager, Integer customerId) {
//        this(nsDao,catalogDao,cacheFileManager);
//        this.customerId = customerId;
//    }

//    private void copyItems(CopyProcessor4 copyProcessor4) throws Exception {
//        copyProcessor4.copyTable(itemCopyDef, new MapSqlParameterSource());
//    }
//
//    private void copyCustomerItemPrices(CopyProcessor4 copyProcessor4, Integer customerId) {
//        MapSqlParameterSource msps = new MapSqlParameterSource();
//        msps.addValue("customerId", customerId);
//        msps.addValue("quantityId", 1);
//        msps.addValue("currencyId", 1);
//        copyProcessor4.copyTable(customerPriceCopyDef, msps);
//    }
//    @Transactional
//    public void updateCacheItems() throws Exception {
//        log.info("upsertCacheItems");
//        int changed = getCatalogDao().upsertCacheItems();
//        log.info("upsertCacheItems changed: " + changed);
//        changed = getCatalogDao().discontinueCacheItems();
//        log.info("upsertCacheItems discontinued: " + changed);
//        changed = getCatalogDao().discontinueCustomerPrices();
//        log.info("discountinueCustomerPrices discontinued: " + changed);
//        //DataSourceUtils.getConnection(getDataSource()).commit();
//    }

//    @Transactional
//    public void updateCacheCustomerPrices(CatalogCustomer catalogCustomer) throws Exception {
//        int changed = getCatalogDao().upsertCustomerPrices(catalogCustomer.getCustomerId());
//        log.info("upsertCustomerPrices changed: " + changed);
//        //DataSourceUtils.getConnection(getDataSource()).commit();
//    }
/*
    @Override
    public void run() {
        log.info("refreshCustomerPriceCache");
        try {
            getNsDao().setDatasource(getNsDataSourceFactory().newNsDataSource(getNsDataSourceFactory().adminDbParams()));
            List<CatalogCustomer> catalogCustomers;
            if (getCustomerId() == null) {
                catalogCustomers = getNsDao().getCatalogCustomers();
            } else {
                catalogCustomers = Collections.singletonList(getNsDao().getCatalogCustomer(getCustomerId()));
            }

            CopyProcessor4 copyProcessor4 = new CopyProcessor4();
            copyProcessor4.setSrcDataSource(getNsDataSourceFactory().newNsDataSource(getNsDataSourceFactory().adminDbParams()));
            copyProcessor4.setDstDataSource(getDataSource());

            int changed;
            copyProcessor4.copyTable(getItemCopyDef(), null);
            getCacheManager().updateCacheItems();
            log.info("upsertCustomerPrices ");

            for (CatalogCustomer catalogCustomer : catalogCustomers) {
                MapSqlParameterSource msps = new MapSqlParameterSource();
                msps.addValue("customerId", catalogCustomer.getCustomerId());
                msps.addValue("quantityId", 1);
                msps.addValue("currencyId", 1);
                copyProcessor4.copyTable(customerPriceCopyDef,msps);
                log.info("upsertCustomerPrices: " + catalogCustomer.getCustomerId());
                getCacheManager().updateCacheCustomerPrices(catalogCustomer);
            }
            log.info("upsertCustomerPrices complete");

            List<Part> baseParts = getCatalogDao().getCachePartsList(false);
            Map<Integer, Part> basePartsMap = new HashMap<Integer, Part>(baseParts.size());
            for (Part part : baseParts) {
                basePartsMap.put(part.getId(), part);
            }
            log.info("Writing cache files to: " + new File(cacheFileManager.getDirectory()).getAbsolutePath());

            Catalog catalog = new Catalog();
            catalog.setPartList(new PartList());
            catalog.getPartList().setPart(baseParts);

            for (CatalogCustomer catalogCustomer : catalogCustomers) {

                log.info("Updating cache file customer " + catalogCustomer.getCustomerId());

                catalog.setCustomerId(catalogCustomer.getCustomerId());

                JAXBContext jaxbContext = JAXBContext.newInstance(Catalog.class);
                Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
                jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, false);

                for (Part basePart : baseParts) {
                    basePart.setCustomerPrice(null);
                    basePart.setCustomerPriceLastModified(null);
                }
                getCatalogDao().assignCacheCustomerPrices(basePartsMap, catalogCustomer.getCustomerId());

                cacheFileManager.updateCustomerCacheFile(jaxbMarshaller, catalog, catalogCustomer.getCustomerId());
            }
            log.info("refreshCustomerPriceCache complete");
        } catch (Throwable ex) {
            log.error(ex.getMessage(), ex);
        }
        log.info("Updating refreshCustomerPriceCache complete");
    }
*/

//    private class CustomerPriceRefreshTask implements Runnable {
//
//        public void run() {
//
//        }
//    }

//    private class ItemRefreshTask implements Runnable {
//
//        public void run() {
//
//        }
//    }

    public void customerRefresh(DataSource nsDataSource) {

        try {
            CopyProcessor4 copyProcessor4 = new CopyProcessor4();
            copyProcessor4.setSrcDataSource(nsDataSource);
            copyProcessor4.setDstDataSource(getDataSource());
            copyProcessor4.copyTable(getCustomerCopyDef(),  new MapSqlParameterSource());
            getCacheManager().updateCacheItems();
        } catch (SQLException ex) {
            log.warn(ex.getMessage(),ex);
        }
    }


    public void itemRefresh(DataSource nsDataSource) {

        try {
            CopyProcessor4 copyProcessor4 = new CopyProcessor4();
            copyProcessor4.setSrcDataSource(nsDataSource);
            copyProcessor4.setDstDataSource(getDataSource());
            copyProcessor4.copyTable(getItemCopyDef(),  new MapSqlParameterSource());
            getCacheManager().updateCacheItems();
        } catch (SQLException ex) {
            log.warn(ex.getMessage(),ex);
        }
    }

    public void customerPriceRefresh(CatalogCustomer catalogCustomer) {

    }

    public void customerPriceRefresh() {

    }

    private class CustomerPricesRefreshTask implements Callable<Integer> {

        private CatalogCustomer catalogCustomer;
        DataSource nsDataSource;

        CustomerPricesRefreshTask(DataSource nsDataSource, CatalogCustomer catalogCustomer) {
            this.catalogCustomer = catalogCustomer;
            this.nsDataSource = nsDataSource;
        }

        public Integer call() {
            try {
                log.info("Starting upsertCustomerPrices customerId: " + catalogCustomer.getCustomerId());
                CopyProcessor4 copyProcessor4 = new CopyProcessor4();
                copyProcessor4.setSrcDataSource(nsDataSource);
                copyProcessor4.setDstDataSource(getDataSource());

                MapSqlParameterSource msps = new MapSqlParameterSource();
                msps.addValue("customerId", catalogCustomer.getCustomerId());
                msps.addValue("quantityId", 1);
                msps.addValue("currencyId", 1);
                copyProcessor4.copyTable(customerPriceCopyDef, msps);
                getCacheManager().updateCacheCustomerPrices(catalogCustomer);
                log.info("Completed upsertCustomerPrices customerId: " + catalogCustomer.getCustomerId());

            } catch (SQLException ex) {
                log.warn(ex.getMessage(),ex);
            }
            return catalogCustomer.getCustomerId();
        }
    }

    private class RefreshTask implements Runnable {

        Integer customerId;

        RefreshTask() {}
        RefreshTask(Integer customerId) {
            this.customerId = customerId;
        }

        public void run() {
            ThreadPoolTaskExecutor priceRefreshExecutor = new ThreadPoolTaskExecutor();
            try {
                priceRefreshExecutor.setThreadGroupName("CustPriceRefreshTask");

                // pool blocks, sharing NS_DB_POOL_SIZE connections amongst all priceRefreshExecutor threads
                // connection pool has memory issues with large ResultSets.

                DataSource nsDataSource = getNsDataSourceFactory().newAdminPooledNsDataSource(NS_DB_POOL_SIZE);
                customerRefresh(nsDataSource);
                itemRefresh(nsDataSource);

                long startTime = System.currentTimeMillis();
                List<Future<Integer>> futures = new LinkedList<>();
                List<CatalogCustomer> catalogCustomers = (customerId == null) ?
                    getCatalogDao().getCatalogCustomers()
                    : Collections.singletonList(getCatalogDao().getCustomer(customerId));

                priceRefreshExecutor.setCorePoolSize(REFRESH_THREADS);
                priceRefreshExecutor.setMaxPoolSize(REFRESH_THREADS);
                priceRefreshExecutor.setQueueCapacity(catalogCustomers.size());
                priceRefreshExecutor.afterPropertiesSet();

                for (CatalogCustomer catalogCustomer : catalogCustomers) {
                    log.debug("Start Customer price cache update: " + catalogCustomer.getCustomerId());
                    futures.add(priceRefreshExecutor.submit(new CustomerPricesRefreshTask(nsDataSource,catalogCustomer)));
                }
                for(Future<Integer> future : futures) {

                    try {
                        log.debug("Future : " + future.get());
                    } catch(Exception ex) {
                        log.warn(ex.getMessage());
                    }
                }
                long endTime = System.currentTimeMillis();
                log.info("Customer price cache update complete, elapsed: " + ((endTime - startTime)) / 1000 + "s");

                log.info("upsertCustomerPrices complete");

                List<Part> baseParts = getCatalogDao().getCachePartsList(false);
                Map<Integer, Part> basePartsMap = new HashMap<Integer, Part>(baseParts.size());
                for (Part part : baseParts) {
                    basePartsMap.put(part.getId(), part);
                }
                log.info("Writing cache files to: " + new File(getCacheFileManager().getDirectory()).getAbsolutePath());

                Catalog catalog = new Catalog();
                catalog.setPartList(new PartList());
                catalog.getPartList().setPart(baseParts);

                for (CatalogCustomer catalogCustomer : catalogCustomers) {

                    log.info("Updating cache file customer " + catalogCustomer.getCustomerId());

                    catalog.setCustomerId(catalogCustomer.getCustomerId());

                    JAXBContext jaxbContext = JAXBContext.newInstance(Catalog.class);
                    Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
                    jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, false);

                    for (Part basePart : baseParts) {
                        basePart.setCustomerPrice(null);
                        basePart.setCustomerPriceLastModified(null);
                    }
                    getCatalogDao().assignCacheCustomerPrices(basePartsMap, catalogCustomer.getCustomerId());

                    getCacheFileManager().updateCustomerCacheFile(jaxbMarshaller, catalog, catalogCustomer.getCustomerId());
                }
                log.info("refreshCustomerPriceCache complete");
            } catch (JAXBException | IOException | SQLException ex) {
                log.warn(ex.getMessage(),ex);
                throw new RuntimeException(ex);
            } finally {
                priceRefreshExecutor.shutdown();
            }
        }
    }

    public void startRefresh(Integer customerId) {
        refreshTaskExecutor.execute(new RefreshTask(customerId));
    }

    public void startRefresh() {
        refreshTaskExecutor.execute(new RefreshTask());
    }
}

