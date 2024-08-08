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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class RefreshNsCustomerPriceCacheJobImpl implements RefreshNsCustomerPriceCacheJob {
    private static final Logger log = LogManager.getLogger(RefreshNsCustomerPriceCacheJobImpl.class);

    private static final int NS_DB_POOL_SIZE = 1;
    private static final int REFRESH_THREADS = 1;

    Map<String, Object> params = new HashMap<String, Object>();

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

    public void customerRefresh(DataSource nsDataSource, String accountId) throws SQLException {

        CopyProcessor4 copyProcessor4 = new CopyProcessor4();
        copyProcessor4.setSrcDataSource(nsDataSource);
        copyProcessor4.setDstDataSource(getDataSource());
        copyProcessor4.copyTable(getCustomerCopyDef(),  params);
        getCacheManager().updateCacheCustomers(accountId);
    }



    public void itemRefresh(DataSource nsDataSource, String accountId) {

        try {
            CopyProcessor4 copyProcessor4 = new CopyProcessor4();
            copyProcessor4.setSrcDataSource(nsDataSource);
            copyProcessor4.setDstDataSource(getDataSource());
            copyProcessor4.copyTable(getItemCopyDef(),  params);
            getCacheManager().updateCacheItems(accountId);
        } catch (SQLException ex) {
            log.warn(ex.getMessage(),ex);
        }
    }

    private class CustomerPricesRefreshTask implements Callable<Integer> {

        private final CatalogCustomer catalogCustomer;
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
                Map<String,Object> callParams = new HashMap<String,Object>(params);
                callParams.put("customerId",catalogCustomer.getCustomerId());
                copyProcessor4.copyTable(customerPriceCopyDef, callParams);
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

        RefreshTask() {
            this.customerId = null;
        }
        RefreshTask(Integer customerId) {
            this.customerId = customerId;
        }


        public void run() {

            String accountId = getNsDataSourceFactory().getNsJdbcAccountId();
            params.put("accountId",accountId);
            params.put("quantityId",1);
            params.put("currencyId",1);

            ThreadPoolTaskExecutor priceRefreshExecutor = new ThreadPoolTaskExecutor();
            try {
                priceRefreshExecutor.setThreadGroupName("CustPriceRefreshTask");

                // pool blocks, sharing NS_DB_POOL_SIZE connections amongst all priceRefreshExecutor threads
                // connection pool has memory issues with large ResultSets.

                DataSource nsDataSource = getNsDataSourceFactory().newAdminPooledNsDataSource(NS_DB_POOL_SIZE);
                customerRefresh(nsDataSource, accountId);
                itemRefresh(nsDataSource, accountId);

                long startTime = System.currentTimeMillis();
                List<Future<Integer>> futures = new LinkedList<>();
                List<CatalogCustomer> catalogCustomers = (customerId == null) ?
                    getCatalogDao().getKeyedCatalogCustomers(accountId)
                    : Collections.singletonList(getCatalogDao().getCatalogCustomer(accountId,customerId));

                priceRefreshExecutor.setCorePoolSize(REFRESH_THREADS);
                priceRefreshExecutor.setMaxPoolSize(REFRESH_THREADS);
                priceRefreshExecutor.setQueueCapacity(catalogCustomers.size());
                priceRefreshExecutor.afterPropertiesSet();

                catalogCustomers.forEach(catalogCustomer -> {
                    log.debug("Customer: " + catalogCustomer.getCustomerId());
                    futures.add(priceRefreshExecutor.submit(new CustomerPricesRefreshTask(nsDataSource,catalogCustomer)));
                });
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

                List<Part> baseParts = getCatalogDao().getCachePartsList(accountId,false);
                Map<Integer, Part> basePartsMap = baseParts.stream().collect(Collectors.toMap(Part::getId, part -> part));

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

                    baseParts.forEach(part -> {part.setCustomerPrice(null); part.setCustomerPriceLastModified(null);});

                    getCatalogDao().assignCacheCustomerPrices(basePartsMap, accountId, catalogCustomer.getCustomerId());

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
        refreshTaskExecutor.execute(new RefreshTask(null));
    }
}

