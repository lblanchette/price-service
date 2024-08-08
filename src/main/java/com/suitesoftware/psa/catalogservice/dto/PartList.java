package com.suitesoftware.psa.catalogservice.dto;


import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlType;
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
