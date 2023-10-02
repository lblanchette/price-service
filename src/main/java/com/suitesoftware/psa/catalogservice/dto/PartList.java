package com.suitesoftware.psa.catalogservice.dto;


import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.List;

/**
 * User: lrb
 * Date: 2/17/11
 * Time: 6:10 PM
 * (c) Copyright Suite Business Software
 */

@XmlType(namespace="http://com.psasecurity")
@XmlRootElement(name="partList")
@XmlAccessorType(XmlAccessType.FIELD)

public class PartList {

    @XmlElement
    List<Part> part;

    public List<Part> getPart() {
        return part;
    }

    public void setPart(List<Part> part) {
        this.part = part;
    }
}
