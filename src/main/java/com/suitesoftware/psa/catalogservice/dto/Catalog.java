package com.suitesoftware.psa.catalogservice.dto;

/**
 * User: lrb
 * Date: 3/14/11
 * Time: 1:05 PM
 * (c) Copyright Suite Business Software
 */

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlType;

/**
 * User: lrb
 * Date: 2/17/11
 * Time: 1:06 PM
 * (c) Copyright Suite Business Software
 */
@XmlType(namespace="http://com.psasecurity")
@XmlRootElement(name="catalog")
@XmlAccessorType(XmlAccessType.FIELD)

public class Catalog {

    @XmlAttribute
    int customerId;

    @XmlAttribute
    String modifiedSince;

    String message;

    PartList partList;

    public Catalog() {}

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public String getModifiedSince() {
        return modifiedSince;
    }

    public void setModifiedSince(String modifiedSince) {
        this.modifiedSince = modifiedSince;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public PartList getPartList() {
        return partList;
    }

    public void setPartList(PartList partList) {
        this.partList = partList;
    }
}
