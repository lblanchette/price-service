package com.suitesoftware.psa.catalogservice;

/**
 * User: lrb
 * Date: 4/1/11
 * Time: 6:02 AM
 * (c) Copyright Suite Business Software
 */
public class CatalogCustomer {

    Integer customerId;
    String name;
    String companyname;
    Integer multiplePriceId;
    String oldId;
    String priceListAccessKey;

    public Integer getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Integer customerId) {
        this.customerId = customerId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCompanyname() {
        return companyname;
    }

    public void setCompanyname(String companyname) {
        this.companyname = companyname;
    }

    public Integer getMultiplePriceId() {
        return multiplePriceId;
    }

    public void setMultiplePriceId(Integer multiplePriceId) {
        this.multiplePriceId = multiplePriceId;
    }

    public String getOldId() {
        return oldId;
    }

    public void setOldId(String oldId) {
        this.oldId = oldId;
    }

    public String getPriceListAccessKey() {
        return priceListAccessKey;
    }

    public void setPriceListAccessKey(String priceListAccessKey) {
        this.priceListAccessKey = priceListAccessKey;
    }
}
