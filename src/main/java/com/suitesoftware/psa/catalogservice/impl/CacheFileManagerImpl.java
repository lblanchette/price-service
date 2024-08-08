package com.suitesoftware.psa.catalogservice.impl;

import com.suitesoftware.psa.catalogservice.dto.Catalog;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import jakarta.ws.rs.core.StreamingOutput;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Date;

public class CacheFileManagerImpl {

    private static final Logger log = LogManager.getLogger(CacheFileManagerImpl.class);

    String directory = "psa_cache";
    final String currentFilePrefix = "current-";
    final String previousFilePrefix = "previous-";
    final String tempFilePrefix = "temp-";

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public String getDirectory() {
        return directory;
    }

    public File getCustomerFile(int customerId) {
        return new File(new File(directory),currentFilePrefix + customerId + ".xml");
    }

    public File getPreviousFile(int customerId) {
        return new File(new File(directory),previousFilePrefix + customerId + ".xml");
    }

    public File getTempFile(int customerId) {
        return new File(new File(directory),tempFilePrefix + customerId + ".xml");
    }

    public void updateCustomerCacheFile(Marshaller jaxbMarshaller, Catalog catalog, Integer customerId)
            throws JAXBException, IOException {
        log.info("Updating refreshCustomerPriceCache " + customerId);
        // write temp
        File currentFile = getCustomerFile(customerId);
        File previousFile = getPreviousFile(customerId);
        File tmpFile = getTempFile(customerId);

        jaxbMarshaller.marshal(catalog,tmpFile);
        if(currentFile.exists()) {
            Files.move(currentFile.toPath(), previousFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        Files.move(tmpFile.toPath(), currentFile.toPath(),StandardCopyOption.REPLACE_EXISTING);
        log.info("refreshCustomerPriceCache complete");
    }

    public Catalog getCustomerCatalog(Integer customerId, File file)
            throws JAXBException {
        log.info("Get getCustomerCatalog from file " + customerId + " " + file.getName());
        JAXBContext jaxbContext = JAXBContext.newInstance( Catalog.class );
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        Catalog catalog = (Catalog)jaxbUnmarshaller.unmarshal(file);
        log.info("getCustomerCatalog complete");
        return catalog;
    }

    public Catalog getCustomerCatalog(Integer customerId)
            throws JAXBException {
        return getCustomerCatalog(customerId,getCustomerFile(customerId));
    }


    public StreamingOutput getCacheFileStreamingOutput(Integer customerId, Date modifiedSince) {
        return new FeedReturnStreamingOutput(getCustomerFile(customerId), modifiedSince);
    }

}
