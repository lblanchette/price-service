package com.suitesoftware.psa.catalogservice;

/**
 * User: lrb
 * Date: 3/14/11
 * Time: 1:05 PM
 * (c) Copyright Suite Business Software
 */

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

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

    PartList partList;

    public Catalog() {}

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public PartList getPartList() {
        return partList;
    }

    public void setPartList(PartList partList) {
        this.partList = partList;
    }
}
