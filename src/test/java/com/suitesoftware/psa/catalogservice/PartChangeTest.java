package com.suitesoftware.psa.catalogservice;

import com.suitesoftware.psa.catalogservice.dto.Catalog;
import com.suitesoftware.psa.catalogservice.dto.Part;
import com.suitesoftware.psa.catalogservice.impl.CacheFileManagerImpl;
//import org.codehaus.jackson.map.ObjectMapper;
//import org.codehaus.jackson.map.ObjectWriter;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PartChangeTest {

    @Test
    public void compareCustomerPrices()  {

        try {
            //List<Integer> customerIds = getCatalogDao().getPriceListCustomersIds();
            //String baseDir = "/Users/lblanchette/dev/projects/price-service/extenserve/";

            CacheFileManagerImpl cfm = new CacheFileManagerImpl();
            Catalog oldCatalog = cfm.getCustomerCatalog(949,new File("/Users/lblanchette/dev/projects/price-service/prod/949-now.xml"));
            Catalog newCatalog = cfm.getCustomerCatalog(949,new File("/Users/lblanchette/dev/projects/price-service/psa_cache/current-949.xml"));

            List<Integer> customerIds = new LinkedList<>();
            customerIds.add(949);

//            for (Integer customerId : customerIds) {
//                System.out.println("Customer " + customerId);
//                Catalog oldCatalog = cacheFileManager.getCustomerCatalog(customerId, new File(baseDir +customerId + "-ext.xml"));
//                Catalog newCatalog = cacheFileManager.getCustomerCatalog(customerId);
                Map<Integer, Part> oldPartMap = new HashMap<>();
                for(Part part : oldCatalog.getPartList().getPart()) {
                    if(part.isDiscontinue()) {
                        continue;
                    }
                    oldPartMap.put(part.getId(),part);
                }
                Map<Integer, Part> newPartMap = new HashMap<>();
                for(Part part : newCatalog.getPartList().getPart()) {
                    if(part.isDiscontinue()) {
                        continue;
                    }
                    newPartMap.put(part.getId(),part);
                }

                if(oldPartMap.size() != newPartMap.size()) {
                    System.out.println("Part Counts differ old: " + oldPartMap.size()  +
                            " new " + newCatalog.getPartList().getPart().size());

                    System.out.println("New part not in Old:");
                    for(Part newPart : newPartMap.values()) {
                        if(!oldPartMap.containsKey(newPart.getId())) {
                            System.out.println(newPart.getId());
                        }
                    }
                    System.out.println("Old part not in New:");
                    for(Part oldPart : oldPartMap.values()) {
                        if(!newPartMap.containsKey(oldPart.getId())) {
                            System.out.println(oldPart.getId());
                        }
                    }
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
//                        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
//                        System.out.println("OLD:\n" + ow.writeValueAsString(oldPart));
//                        System.out.println("NEW:\n" + ow.writeValueAsString(newPart));
                        System.out.println(oldPart.getId() + "," + oldPart.getPrice().toPlainString() + "," + newPart.getPrice().toPlainString());
                    }
                }
//            }
        } catch(Throwable ex) {
            ex.printStackTrace();
        }
    }

}
