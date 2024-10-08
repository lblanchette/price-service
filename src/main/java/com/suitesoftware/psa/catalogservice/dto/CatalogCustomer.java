package com.suitesoftware.psa.catalogservice.dto;

/**
 * User: lrb
 * Date: 4/1/11
 * Time: 6:02 AM
 * (c) Copyright Suite Business Software
 */
public class CatalogCustomer {

    Integer id;
    String accountId;
    Integer customerId;
    Integer parentId;
    String isInactive;
    String name;
    String companyname;
    Integer multiplePriceId;
    String oldId;
    String priceListAccessKey;

    public Integer getId() {
        return id == null ? customerId : id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public Integer getCustomerId() {
        return customerId != null ? customerId : id;
    }

    public void setCustomerId(Integer customerId) {
        this.customerId = customerId;
    }

    public Integer getParentId() {
        return parentId;
    }

    public void setParentId(Integer parentId) {
        this.parentId = parentId;
    }

    public String getIsInactive() {
        return isInactive;
    }

    public void setIsInactive(String isInactive) {
        this.isInactive = isInactive;
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

    public Boolean isCaching() {
        return priceListAccessKey != null && priceListAccessKey.length() > 0;
    }

    public void setPriceListAccessKey(String priceListAccessKey) {
        this.priceListAccessKey = priceListAccessKey;
    }
}
